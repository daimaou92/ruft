use anyhow::{bail, Result};
use clap::Parser;
use futures::{
    future::{self, Ready},
    io::Read,
    prelude::*,
};
use rand::Rng;
use std::{
    collections::{HashMap, HashSet},
    net::{IpAddr, SocketAddr, SocketAddrV4, SocketAddrV6},
    str::{FromStr, Matches},
    sync::{Arc, RwLock},
    time::Duration,
};
use tarpc::{
    client,
    context,
    serde::{Deserialize, Serialize},
    server::{self, incoming::Incoming, Channel},
    tokio_serde::formats::Json,
    // transport::channel,
};
use tokio::{
    sync::{mpsc, Mutex},
    time,
};

#[derive(Debug, Default, Clone, Serialize, Deserialize)]
struct Log {
    cmd: String,
    term: u32,
}

#[derive(Debug, Clone)]
enum NodeState {
    Candidate,
    Follower,
    Leader,
}

impl Default for NodeState {
    fn default() -> Self {
        NodeState::Follower
    }
}

type NodeID = (String, u16);

fn ni_to_sa(ni: &NodeID) -> Result<SocketAddr> {
    let ip = IpAddr::from_str(ni.0.as_str())?;
    Ok(SocketAddr::from((ip, ni.1)))
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct Election {
    term: u32,
    candidate_id: NodeID,
    last_log_index: u64,
    last_log_term: u32,
}

impl Election {
    pub fn new(term: u32, candidate_id: NodeID, last_log_index: u64, last_log_term: u32) -> Self {
        Self {
            term,
            candidate_id,
            last_log_index,
            last_log_term,
        }
    }
}

#[derive(Debug)]
struct ElectionResult {
    election: Arc<Election>,
    votes_received: Vec<NodeID>,
}

impl ElectionResult {
    fn new(election: Arc<Election>) -> Self {
        ElectionResult {
            election,
            votes_received: Vec::<NodeID>::new(),
        }
    }
}

#[derive(Debug, Default, Clone)]
struct Cluster {
    members: HashSet<NodeID>,
    leader: NodeID,
    majority: u16,
}

#[derive(Debug, Clone)]
struct Node {
    id: NodeID,
    leader: NodeID,
    cluster: Arc<Cluster>,
    // leader_timeout_millis: u32,
    state: NodeState,
    last_rpc_instant: tokio::time::Instant,
    // persistent state
    logs: Vec<Log>,
    current_term: u32,
    voted_log: HashMap<u32, NodeID>,
    // volatile state
    commit_index: u64,
    last_applied: u64,
    // volatile state leaders
    next_indices: HashMap<NodeID, u64>,
    match_indices: HashMap<NodeID, u64>,
}
impl Default for Node {
    fn default() -> Self {
        Node {
            id: (String::from(""), 0),
            leader: (String::from(""), 0),
            state: NodeState::Follower,
            last_rpc_instant: tokio::time::Instant::now(),
            logs: vec![Log::default()], // Index starts at 1
            current_term: 0,
            voted_log: HashMap::new(),
            commit_index: 0,
            last_applied: 0,
            next_indices: HashMap::new(),
            match_indices: HashMap::new(),
            cluster: Arc::new(Cluster::default()),
        }
    }
}

impl Node {
    pub fn new(ip: String, port: u16, cluster: Arc<Cluster>) -> Self {
        Self {
            id: (ip, port),
            cluster,
            ..Default::default()
        }
    }

    pub fn voted_for(&self, term: u32) -> Option<&NodeID> {
        self.voted_log.get(&term)
    }

    pub fn is_leader(&self) -> bool {
        matches!(self.state, NodeState::Leader)
    }

    pub fn deserves_vote(&self, e: &Election) -> Result<bool> {
        if self.current_term > e.term {
            return Ok(false);
        }
        let d = match self.voted_for(e.term) {
            Some(x) => (e.candidate_id == *x),
            None => true,
        };
        // TODO
        if d && self.last_applied <= e.last_log_index {
            return Ok(true);
        }
        Ok(false)
    }
}

#[derive(Serialize, Deserialize, Debug)]
struct AppendEntries {
    term: u32,
    leader_id: NodeID,
    prev_log_index: u64,
    prev_log_term: u32,
    entries: Vec<Log>,
    leader_commit: u64,
}

#[tarpc::service]
trait NodeRPC {
    async fn request_vote(e: Election) -> (u32, bool);
    async fn append_entries(ae: AppendEntries) -> (u32, bool);
}

async fn client_from_node_id(ni: &NodeID) -> Result<NodeRPCClient> {
    let sa = ni_to_sa(ni)?;
    let transport = tarpc::serde_transport::tcp::connect(sa, Json::default);
    let client = NodeRPCClient::new(client::Config::default(), transport.await?).spawn();
    Ok(client)
}

#[derive(Debug, Clone)]
struct NodeServer {
    node: Arc<RwLock<Node>>,
    cluster: Arc<Cluster>,
}

impl NodeServer {
    fn new(n: Arc<RwLock<Node>>, clu: Arc<Cluster>) -> Self {
        NodeServer {
            node: n,
            cluster: clu,
        }
    }
}

#[tarpc::server]
impl NodeRPC for NodeServer {
    async fn request_vote(self, _: context::Context, e: Election) -> (u32, bool) {
        let mut node = match self.node.write() {
            Ok(n) => n,
            Err(v) => {
                println!("[request_vote] -> {}", v);
                return (e.term, false);
            }
        };
        if !(matches!(node.state, NodeState::Follower)) {
            return (e.term, false);
        }

        let dv = match node.deserves_vote(&e) {
            Ok(b) => b,
            Err(v) => {
                println!("[request_vote] -> {}", v);
                return (e.term, false);
            }
        };
        if node.current_term < e.term {
            node.current_term = e.term;
        }
        (e.term, dv)
    }

    async fn append_entries(self, _: context::Context, ae: AppendEntries) -> (u32, bool) {
        // This is always sent by the leader
        let mut node = match self.node.write() {
            Ok(n) => n,
            Err(e) => {
                println!("[append_entries] -> {}", e);
                return (ae.term, false);
            }
        };
        node.last_rpc_instant = tokio::time::Instant::now();

        if ae.term < node.current_term {
            return (ae.term, false);
        }

        {
            let log = match node.logs.get(ae.prev_log_index as usize) {
                Some(l) => l,
                None => {
                    return (ae.term, false);
                }
            };
            if log.term != ae.prev_log_term {
                return (ae.term, false);
            }
        }

        let mut offset: usize = 0;
        for (i, e) in ae.entries.iter().enumerate() {
            let log = match node.logs.get((ae.prev_log_index + 1) as usize + i) {
                Some(l) => l,
                None => {
                    offset = i;
                    break;
                }
            };
            if log.term != e.term {
                offset = i;
                break;
            }
        }

        if node.logs.len() > ae.prev_log_index as usize + offset + 2 {
            for i in ae.prev_log_index as usize + 1 + offset..node.logs.len() {
                node.logs.remove(i);
            }
            node.last_applied = ae.prev_log_index + offset as u64;
        }

        for i in offset..ae.entries.len() {
            let l = match ae.entries.get(i) {
                Some(l) => l,
                None => {
                    return (ae.term, false);
                }
            };
            node.logs.push(l.clone());
            node.last_applied = node.logs.len() as u64 - 1;
        }
        if ae.leader_commit > node.commit_index {
            if ae.leader_commit > node.last_applied {
                node.commit_index = node.last_applied;
            } else {
                node.commit_index = ae.leader_commit;
            }
        }
        (ae.term, true)
    }
}

#[derive(Clone)]
struct NodeClient {
    election_timeout: tokio::time::Duration,
    rpc_timeout: tokio::time::Duration,
    node: Arc<RwLock<Node>>,
    cluster: Arc<Cluster>,
}

impl NodeClient {
    fn new(amn: Arc<RwLock<Node>>, clu: Arc<Cluster>) -> Self {
        let mut rng = rand::thread_rng();
        let n: u64 = rng.gen_range(250..=500);
        NodeClient {
            election_timeout: tokio::time::Duration::from_millis(n),
            rpc_timeout: tokio::time::Duration::from_millis(20),
            node: amn,
            cluster: clu,
        }
    }

    fn get_new_election(&self) -> Result<Election> {
        let mut last_log_term: u32 = 0;
        {
            let node = match self.node.as_ref().read() {
                Ok(n) => n,
                Err(v) => {
                    bail!(v.to_string());
                }
            };

            if node.last_applied > 0 {
                println!("[get_new_election] -> last_apploed is greater than 0");
                let log = node.logs.get(node.last_applied as usize);
                if log.is_none() {
                    bail!("log doesn't exist at last_applied index");
                }
                last_log_term = log.unwrap().term;
            }
        } // Read lock drops here before write lock is taken below

        let mut node = match self.node.as_ref().write() {
            Ok(n) => n,
            Err(v) => {
                bail!(v.to_string());
            }
        };
        node.current_term += 1;
        Ok(Election::new(
            node.current_term,
            node.id.clone(),
            node.last_applied,
            last_log_term,
        ))
    }
}

async fn monitor_election(nc: NodeClient, tx: mpsc::Sender<bool>) {
    let elec_sleep = time::sleep(nc.election_timeout);
    tokio::pin!(elec_sleep);
    loop {
        tokio::select! {
            () = &mut elec_sleep => {
                // println!("timer elapsed");
                let mut is_candidate = false;
                let mut term: u32 = 0;
                let mut id: NodeID = (String::from(""), 0);
                let mut rd = nc.election_timeout.as_millis() as u64;
                if let Ok(node) = nc.node.as_ref().read() {
                    let d = elec_sleep.deadline().duration_since(
                        node.last_rpc_instant,
                        ).as_millis();
                    if d > nc.election_timeout.as_millis() {
                       is_candidate = true;
                       term = node.current_term;
                       id = node.id.clone();
                    } else {
                        rd = (nc.election_timeout.as_millis() - d) as u64;
                    }
                }

                if is_candidate {
                    if let Ok(mut node) = nc.node.as_ref().write() {
                        node.current_term += 1;
                        (&mut node.voted_log).insert(term, id);
                        node.last_rpc_instant = tokio::time::Instant::now();
                        node.state = NodeState::Candidate;
                        let tx1 = tx.clone();
                        println!("Election for term: {}", node.current_term);
                        tokio::spawn(async move{
                            match tx1.send(true).await {
                                Ok(_) => {},
                                Err(e) => {println!("{}",e)}
                            }
                        });
                    }
                }
                elec_sleep.as_mut().reset(
                    tokio::time::Instant::now() +
                    Duration::from_millis(rd)
                );
            },
        }
    }
}

async fn as_candidate(nc: NodeClient, mut rx: mpsc::Receiver<bool>) {
    async fn execute(node_id: NodeID, e: Election, er: Arc<Mutex<ElectionResult>>) -> bool {
        let t = std::time::SystemTime::now().checked_add(std::time::Duration::from_millis(20));
        if t.is_none() {
            println!("[as_candidate.execute] -> could not get system time for context deadline");
            return false;
        }

        let mut ctx = context::current();
        ctx.deadline = t.unwrap();

        let rpcl = match client_from_node_id(&node_id).await {
            Ok(x) => x,
            Err(v) => {
                println!("[as_candidate.execute] -> {}", v);
                return false;
            }
        };

        let (_, success) = match rpcl.request_vote(ctx, e).await {
            Ok((t, s)) => (t, s),
            Err(v) => {
                println!("[as_candidate.execute] -> {}", v);
                return false;
            }
        };
        // TODO
        if success {
            let mut erguard = er.as_ref().lock().await;
            erguard.votes_received.push(node_id);
        } else {
            println!(
                "[as_candidate.execute] -> didn't get vote for {:?}",
                node_id
            );
        }
        false
    }

    let self_id = match nc.node.clone().read() {
        Ok(n) => n.id.clone(),
        Err(e) => {
            println!("[as_candidate] -> {}", e);
            return;
        }
    };

    let election = match nc.get_new_election() {
        Ok(e) => e,
        Err(v) => {
            println!("[as_candidate.execute] -> {}", v);
            return;
        }
    };
    let e1 = Arc::new(election.clone());
    let election_result = Arc::new(Mutex::new(ElectionResult::new(e1)));
    while let Some(_b) = rx.recv().await {
        // println!("[as_cndidate] -> Got: {}", b);
        let members = nc.cluster.members.clone();
        let mut fj = Vec::new();
        for x in members.into_iter() {
            if self_id == x {
                continue;
            }
            let node_id = x.clone();
            let e = election.clone();
            let er = election_result.clone();
            fj.push(tokio::spawn(async move {
                execute(node_id, e, er).await;
            }));
        }
        futures::future::join_all(fj).await;
        let er = election_result.lock().await;
        if er.votes_received.len() >= nc.cluster.majority as usize {}
    }
}

async fn as_leader(nc: NodeClient) {
    let mut interval = tokio::time::interval(nc.rpc_timeout);

    tokio::select! {
        _ = interval.tick() => {
            if let Ok(node) = nc.node.as_ref().read() {
                if !node.is_leader() {
                    return;
                }
            } else {
                return;
            }
            // Do leader empty rpcs here
        }
    }
}

#[derive(Parser, Debug)]
#[clap(author,version,about,long_about=None)]
struct Args {
    #[clap(short, long, default_value = "127.0.0.1")]
    listen: String,

    #[clap(short, long, default_value_t = 20000)]
    port: u16,

    #[clap(
        short,
        long,
        default_value = "127.0.0.1:20000,127.0.0.1:20001,127.0.0.1:20002"
    )]
    cluster: String,
}

async fn start_server(
    node: Arc<RwLock<Node>>,
    cluster: Arc<Cluster>,
    node_id: NodeID,
) -> Result<()> {
    let server_addr = ni_to_sa(&node_id)?;

    // JSON transport is provided by the json_transport tarpc module. It makes it easy
    // to start up a serde-powered json serialization strategy over TCP.
    let mut listener = tarpc::serde_transport::tcp::listen(&server_addr, Json::default).await?;
    listener.config_mut().max_frame_length(usize::MAX);
    listener
        // Ignore accept errors.
        .filter_map(|r| future::ready(r.ok()))
        .map(server::BaseChannel::with_defaults)
        // Limit channels to 1 per IP.
        .max_channels_per_key(1, |t| t.transport().peer_addr().unwrap().ip())
        // serve is generated by the service attribute. It takes as input any type implementing
        // the generated World trait.
        .map(|channel| {
            let ns = NodeServer::new(node.clone(), cluster.clone());
            channel.execute(ns.serve())
        })
        // Max 10 channels.
        .buffer_unordered(10)
        .for_each(|_| async {})
        .await;
    Ok(())
}

#[tokio::main]
async fn main() -> Result<()> {
    let args = Args::parse();

    let mut clu = Cluster::default();
    // let spl = ;
    for x in args.cluster.split(',') {
        let mut err = false;
        if let Ok(a) = SocketAddrV4::from_str(x) {
            clu.members.insert((a.ip().to_string(), a.port()));
        } else {
            err = true;
        }
        if err {
            if let Ok(a) = SocketAddrV6::from_str(x) {
                clu.members.insert((a.ip().to_string(), a.port()));
            } else {
                bail!("something went wrong")
            }
        }
    }
    clu.majority = clu.members.len() as u16 / 2 + 1;

    let mut fts = vec![];

    let arclu = Arc::new(clu);
    let node = Node::new(args.listen, args.port, arclu.clone());
    let nid = node.id.clone();
    let amn = Arc::new(RwLock::new(node));

    let (tx, rx) = mpsc::channel(1);

    // Start server
    let amn1 = amn.clone();
    let arclu1 = arclu.clone();
    let nid1 = nid.clone();
    fts.push(tokio::spawn(async move {
        match start_server(amn1, arclu1, nid1).await {
            Ok(_) => {}
            Err(e) => {
                println!("[main] -> Server couldn't be started: {}", e)
            }
        };
    }));

    let nc = NodeClient::new(amn, arclu);
    let nc1 = nc.clone();
    fts.push(tokio::spawn(async move {
        monitor_election(nc1, tx.clone()).await;
    }));

    let nc2 = nc.clone();
    // let cl1 = cl.clone();
    fts.push(tokio::spawn(async move {
        as_candidate(nc2, rx).await;
    }));

    fts.push(tokio::spawn(async move {
        as_leader(nc).await;
    }));

    futures::future::join_all(fts).await;
    // start client server here as well
    Ok(())
}
