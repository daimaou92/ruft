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
    sync::Arc,
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
    sync::{mpsc, Mutex, RwLock},
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

impl std::fmt::Display for NodeState {
    fn fmt(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        if matches!(self, NodeState::Follower) {
            return write!(f, "follower");
        } else if matches!(self, NodeState::Candidate) {
            return write!(f, "candidate");
        } else if matches!(self, NodeState::Leader) {
            return write!(f, "leader");
        }
        write!(f, "invalid_state")
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
    last_leader_quorum: tokio::time::Instant,
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
            last_leader_quorum: tokio::time::Instant::now(),
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

    // pub fn is_leader(&self) -> bool {
    //     matches!(self.state, NodeState::Leader)
    // }

    pub fn deserves_vote(&self, e: &Election) -> bool {
        if self.current_term > e.term {
            return false;
        }
        let d = match self.voted_for(e.term) {
            Some(x) => (e.candidate_id == *x),
            None => true,
        };
        if !d {
            return false;
        }

        let last_index = self.logs.len() - 1;
        let mut last_log_term = 0;
        if let Some(l1) = self.logs.get(last_index) {
            last_log_term = l1.term;
        }
        let mut up_to_date = false;
        if e.last_log_term > last_log_term {
            up_to_date = true;
        }
        if e.last_log_term == last_log_term && e.last_log_index >= last_index as u64 {
            up_to_date = true;
        }

        if up_to_date {
            return true;
        }
        false
    }

    // Increases rpc_instant
    pub fn append_logs(&mut self, logs: Vec<Log>) {
        self.last_rpc_instant = tokio::time::Instant::now();
        if logs.is_empty() {
            return;
        }
        for l in logs.iter() {
            self.logs.push(l.clone());
        }
        self.last_applied = self.logs.len() as u64 - 1;
    }

    pub fn update_commit_index(&mut self, mut from: u64, to: u64, majority: u16) {
        if from > to || self.commit_index >= to {
            return;
        }

        if self.commit_index > from {
            from = self.commit_index;
        }

        for i in from..to + 1 {
            let mut count: u16 = 0;
            for (_, v) in self.match_indices.iter() {
                if *v > i {
                    count += 1;
                }
                if count >= majority {
                    self.commit_index = i;
                    break;
                }
            }
        }
    }
}

#[derive(Serialize, Deserialize, Debug, Default)]
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
    async fn append_entries(ae: AppendEntries) -> (u32, bool, u64);
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
        // println!(
        //     "[request_vote] -> fired for term: {} and candidate: {:?}",
        //     e.term, e.candidate_id,
        // );
        let mut node = self.node.write().await;
        if e.term > node.current_term {
            node.state = NodeState::Follower;
            node.current_term = e.term;
            node.last_rpc_instant = tokio::time::Instant::now();
        }
        let dv = node.deserves_vote(&e);
        // let node_id = e.candidate_id.clone();
        node.voted_log.insert(e.term, e.candidate_id);
        // println!(
        //     "[request_vote] -> voted for term: {} and candidate: {:?}",
        //     e.term, node_id,
        // );
        (e.term, dv)
    }

    async fn append_entries(self, _: context::Context, ae: AppendEntries) -> (u32, bool, u64) {
        // This is always sent by the leader
        println!("append_entries received from: [{:?}]", ae.leader_id);

        {
            let mut node = self.node.write().await;
            if ae.term < node.current_term {
                return (ae.term, false, node.last_applied);
            }

            if ae.term > node.current_term {
                node.current_term = ae.term;
                node.state = NodeState::Follower;
            }

            node.last_rpc_instant = tokio::time::Instant::now();
        }

        let mut offset: usize = 0;
        {
            let node = self.node.read().await;
            let log = match node.logs.get(ae.prev_log_index as usize) {
                Some(l) => l,
                None => {
                    return (ae.term, false, node.last_applied);
                }
            };
            if log.term != ae.prev_log_term {
                return (ae.term, false, node.last_applied);
            }

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
        }

        {
            let mut node = self.node.write().await;
            if node.logs.len() > ae.prev_log_index as usize + offset + 2 {
                for i in ae.prev_log_index as usize + 1 + offset..node.logs.len() {
                    node.logs.remove(i);
                }
                node.last_applied = ae.prev_log_index + offset as u64;
            }

            let mut entries = Vec::new();
            for i in offset..ae.entries.len() {
                if let Some(v) = ae.entries.get(i) {
                    entries.push(v.clone());
                }
            }
            node.append_logs(entries);
            if ae.leader_commit > node.commit_index {
                if ae.leader_commit > node.last_applied {
                    node.commit_index = node.last_applied;
                } else {
                    node.commit_index = ae.leader_commit;
                }
            }
            (ae.term, true, node.last_applied)
        }
    }
}

#[derive(Clone)]
struct NodeClient {
    election_timeout: tokio::time::Duration,
    rpc_timeout: tokio::time::Duration,
    leader_lease_timeout: tokio::time::Duration,
    node: Arc<RwLock<Node>>,
    cluster: Arc<Cluster>,
}

impl NodeClient {
    fn new(amn: Arc<RwLock<Node>>, clu: Arc<Cluster>) -> Self {
        let mut rng = rand::thread_rng();
        let n: u64 = rng.gen_range(5000..=10000);
        NodeClient {
            election_timeout: tokio::time::Duration::from_millis(n),
            rpc_timeout: tokio::time::Duration::from_millis(1000),
            leader_lease_timeout: tokio::time::Duration::from_millis(10000),
            node: amn,
            cluster: clu,
        }
    }

    async fn get_new_election(&self) -> Election {
        let mut last_log_term: u32 = 0;
        let mut term = 0;
        let mut last_applied = 0;
        let mut node_id = NodeID::default();
        {
            let node = self.node.read().await;
            term = node.current_term;
            if node.last_applied > 0 {
                println!("[get_new_election] -> last_apploed is greater than 0");
                if let Some(log) = node.logs.get(node.last_applied as usize) {
                    last_log_term = log.term;
                }
            }
            last_applied = node.last_applied;
            node_id = node.id.clone();
        } // Read lock drops here before write lock is taken below

        Election::new(term, node_id, last_applied, last_log_term)
    }

    pub async fn send_append_rpc(&self) {
        let mut self_id = NodeID::default();
        {
            let mut node = self.node.write().await;
            if !matches!(node.state, NodeState::Leader) {
                println!(
                    "[send_append_rpc] -> only leader can send rpc. this is: {:?}",
                    node.state,
                );
                return;
            }
            node.last_rpc_instant = tokio::time::Instant::now();
            self_id = node.id.clone();
        }

        let success_from = Arc::new(Mutex::<u16>::new(0));
        let majority_achieved = Arc::new(Mutex::new(false));
        for m in self.cluster.members.iter() {
            if self_id == *m {
                continue;
            }
            let arc_node = self.node.clone();
            let m = m.clone();
            let majority = self.cluster.majority;
            let rpc_timeout = self.rpc_timeout;
            let sf = success_from.clone();
            let ma = majority_achieved.clone();
            tokio::spawn(async move {
                let mut ae = AppendEntries::default();
                {
                    let node = arc_node.read().await;
                    ae.term = node.current_term;
                    ae.leader_id = node.id.clone();
                    ae.prev_log_index = match node.match_indices.get(&m) {
                        Some(i) => *i,
                        None => node.logs.len() as u64 - 1,
                    };
                    if ae.prev_log_index > 0 {
                        if let Some(l) = node.logs.get(ae.prev_log_index as usize) {
                            ae.prev_log_term = l.term;
                        }
                    }

                    for i in (ae.prev_log_index as usize + 1)..node.logs.len() {
                        let l = match node.logs.get(i) {
                            Some(l) => l,
                            None => {
                                continue;
                            }
                        };
                        ae.entries.push(l.clone());
                    }
                    ae.leader_commit = node.commit_index
                }

                let cli = match client_from_node_id(&m).await {
                    Ok(cli) => cli,

                    Err(_v) => {
                        // println!("member [{:?}] unreachable", &m);
                        return;
                    }
                };

                let mut ctx = context::current();
                if let Some(t) = std::time::SystemTime::now().checked_add(rpc_timeout) {
                    ctx.deadline = t;
                }

                let from = ae.prev_log_index + 1;
                if let Ok((_, success, last_index)) = cli.append_entries(ctx, ae).await {
                    // println!("[NodeClient.send_append_rpc] -> responded with success: {}, last_index: {}", success, last_index);
                    let mut node = arc_node.write().await;
                    node.next_indices.insert(m.clone(), last_index);
                    node.match_indices.insert(m.clone(), last_index);
                    if success {
                        node.update_commit_index(from, last_index, majority);
                        let mut update_lease = false;
                        {
                            let mut d = sf.lock().await;
                            *d += 1;

                            if (*d + 1) >= majority {
                                let mut v = ma.lock().await;
                                if !*v {
                                    update_lease = true;
                                    *v = true;
                                }
                            }
                        }
                        if update_lease {
                            // let mut n = arc_node.write().await;
                            node.last_leader_quorum = tokio::time::Instant::now();
                        }
                    }
                }
            });
        }
    }
}

async fn monitor_election(nc: Arc<NodeClient>, tx: mpsc::Sender<bool>) {
    loop {
        let mut d = Duration::from_secs(0);
        {
            let last_rpc = nc.node.read().await.last_rpc_instant;
            d = tokio::time::Instant::now().duration_since(last_rpc)
        }
        // println!("[monitor_election] -> duration since: {:?}", d);
        if d > nc.election_timeout {
            // println!("[monitor_election] -> duration since: {:?}", d);
            {
                let mut node = nc.node.write().await;
                node.last_rpc_instant = tokio::time::Instant::now();
            }
            match tx.send(true).await {
                Ok(_) => {}
                Err(e) => {
                    println!("{}", e)
                }
            }
        }
    }
}

async fn monitor_leader_lease(nc: Arc<NodeClient>) {
    loop {
        if !matches!(nc.node.read().await.state, NodeState::Leader) {
            continue;
        }
        let mut d = Duration::from_secs(0);
        {
            let last_q = nc.node.read().await.last_leader_quorum;
            d = tokio::time::Instant::now().duration_since(last_q);
        }
        if d > nc.leader_lease_timeout {
            println!("\n#####################\nStepping down from Leader\n#####################\n");
            {
                let mut node = nc.node.write().await;
                node.state = NodeState::Follower;
            }
        }
    }
}

async fn as_candidate(
    nc: Arc<NodeClient>,
    mut rx: mpsc::Receiver<bool>,
    leader_tx: mpsc::Sender<bool>,
) {
    async fn execute(
        node_id: &NodeID,
        e: Election,
        received_from: Arc<Mutex<Vec<NodeID>>>,
        rpc_timeout: Duration,
    ) {
        let mut ctx = context::current();
        if let Some(t) = std::time::SystemTime::now().checked_add(rpc_timeout) {
            ctx.deadline = t;
        }

        let rpcl = match client_from_node_id(node_id).await {
            Ok(x) => x,
            Err(_v) => {
                // println!("member [{:?}] unreachable", node_id);
                return;
            }
        };

        let (_term, success) = match rpcl.request_vote(ctx, e).await {
            Ok((t, s)) => (t, s),
            Err(v) => {
                println!("member [{:?}] -> failed to vote coz: {}", node_id, v);
                return;
            }
        };
        // println!(
        //     "member [{:?}] vote response: {{ \"Term\": {}, \"Success\": {} }}",
        //     node_id, term, success
        // );

        if success {
            println!("member [{:?}] voted", node_id);
            let mut v = received_from.lock().await;
            v.push(node_id.clone());
        } else {
            println!("member [{:?}] didnot voted", node_id);
        }
    }

    let mut self_id = NodeID::default();
    {
        self_id = nc.node.read().await.id.clone();
    }
    while let Some(_b) = rx.recv().await {
        {
            let mut node = nc.node.write().await;
            node.state = NodeState::Candidate;
            node.current_term += 1;
            let term = node.current_term;
            let id = node.id.clone();
            node.voted_log.insert(term, id);
            node.last_rpc_instant = tokio::time::Instant::now();
            println!(
                "\n#########################\nCandidate for Term: {}\n#########################\n",
                node.current_term
            );
        }
        let election = nc.get_new_election().await;

        let received_from = Arc::new(Mutex::new(vec![self_id.clone()]));
        let members = nc.cluster.members.clone();
        // let mut fj = Vec::new();
        let majority_reached = Arc::new(Mutex::new(false));
        for x in members.into_iter() {
            if self_id == x {
                continue;
            }
            // let node_id = x.clone();
            let e = election.clone();
            let rf = received_from.clone();
            let majority = nc.cluster.majority;
            let mr = majority_reached.clone();
            // let node = nc.node.clone();
            let tx = leader_tx.clone();
            let nc1 = nc.clone();
            tokio::spawn(async move {
                // let er1 = er.clone();
                execute(&x, e, rf.clone(), nc1.rpc_timeout).await;
                let rfv = rf.lock().await;
                if rfv.len() >= majority as usize {
                    let mut do_leader_stuff = false;
                    {
                        let mut v = mr.lock().await;
                        if !*v {
                            do_leader_stuff = true;
                            *v = true;
                        }
                    }
                    if do_leader_stuff {
                        match tx.send(true).await {
                            Ok(_) => {
                                println!(
                                    "\n#########################\nLEADER\n#########################\n"
                                );
                            }
                            Err(v) => {
                                println!("Error sending leader chan: {}", v);
                            }
                        };
                    }
                }
            });
        }
    }
}

async fn as_leader(nc: Arc<NodeClient>) {
    let mut interval = tokio::time::interval(nc.rpc_timeout);

    loop {
        let _i = interval.tick().await;
        {
            let node = nc.node.read().await;
            if !matches!(node.state, NodeState::Leader) {
                continue;
            }
        }

        nc.send_append_rpc().await;
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

    // Arc node for sharing
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

    let amn2 = amn.clone();
    let anc = Arc::new(NodeClient::new(amn2, arclu));
    let anc1 = anc.clone();
    fts.push(tokio::spawn(async move {
        monitor_election(anc1, tx).await;
    }));

    let (l_tx, mut l_rx) = mpsc::channel(1);
    let anc2 = anc.clone();
    // let cl1 = cl.clone();
    fts.push(tokio::spawn(async move {
        as_candidate(anc2, rx, l_tx).await;
    }));
    let anc3 = anc.clone();
    fts.push(tokio::spawn(async move {
        while let Some(_b) = l_rx.recv().await {
            {
                let mut node = amn.write().await;
                node.state = NodeState::Leader;
                node.last_rpc_instant = tokio::time::Instant::now();
                node.last_leader_quorum = tokio::time::Instant::now();
            }
            anc3.send_append_rpc().await;
        }
    }));

    let anc4 = anc.clone();
    fts.push(tokio::spawn(async move {
        monitor_leader_lease(anc4).await;
    }));

    fts.push(tokio::spawn(async move {
        as_leader(anc).await;
    }));

    futures::future::join_all(fts).await;
    // start client server here as well
    Ok(())
}
