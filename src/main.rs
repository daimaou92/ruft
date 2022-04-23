use anyhow::{Result, bail};
use rand::Rng;
use tokio::{time,sync::mpsc};
use std::{collections::{HashMap, HashSet}, net::IpAddr, str::FromStr, sync::{
    Arc, RwLock,
}, time::Duration};
use futures::{
    future::{self, Ready},
    prelude::*, io::Read,
};
use tarpc::{
    client, context,
    server::{self, incoming::Incoming, Channel},
    tokio_serde::formats::Json, transport::channel,
    serde::{Serialize,Deserialize},
};
use clap::Parser;

#[derive(Debug, Default, Clone,Serialize, Deserialize)]
struct Log {
    cmd: String,
    term: u32,
}

#[derive(Debug,Clone)]
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

#[derive(Debug,Serialize,Deserialize)]
struct Election {
    term: u32,
    candidate_id: NodeID,
    last_log_index: u64,
    last_log_term: u32,
}

impl Election {
    pub fn new(
        term: u32, candidate_id: NodeID, last_log_index: u64, last_log_term: u32,
    ) -> Self {
        Self{
            term, candidate_id, last_log_index, last_log_term,
        }
    }
}


#[derive(Debug, Clone)]
struct Node {
    id: NodeID,
    leader: NodeID,
    leader_timeout_millis: u32,
    state: NodeState,
    last_rpc_instant: tokio::time::Instant,
    // persistent state
    logs: Vec<Log>,
    current_term: u32,
    voted_log: HashMap<u32,NodeID>,
    // volatile state
    commit_index: u64,
    last_applied: u64,
    // volatile state leaders
    next_indices: HashMap<NodeID, u64>,
    match_indices: HashMap<NodeID, u64>
}
impl Default for Node {
    fn default() -> Self {
        Node{
            last_rpc_instant: tokio::time::Instant::now(),
            ..Default::default()
        }
    }
}

impl Node {
    pub fn new(ip: String, port: u16) -> Self {
        Self {
            id: (ip, port),
            ..Default::default()
        }
    }

    pub fn voted_for(&self, term: u32) -> Option<&NodeID> {
        self.voted_log.get(&term)
    }

    pub fn leader_who(&self) -> Option<&NodeID> {
        if self.is_leader() {
            return Some(&self.id);
        } else if self.leader != (String::from(""), 0) {
            return Some(&self.leader);
        }
        None
    }

    pub fn is_leader(&self) -> bool {
        matches!(self.state, NodeState::Leader)
    }

    pub fn deserves_vote(&self, e: &Election) -> Result<bool> {
        if self.current_term > e.term {
            return Ok(false);
        }
        let mut d = false;
        match self.voted_for(e.term) {
            Some(x) => {
                if e.candidate_id == *x {
                    d = true;
                }
            },
            None => {
                d = true;
            },
        }
        // TODO
        if d && self.last_applied <= e.last_log_index {
            return Ok(true);
        }
        Ok(false)
    }
}

#[derive(Serialize,Deserialize,Debug)]
struct AppendEntries {
    term:u32,
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

#[derive(Debug,Clone)]
struct NodeServer {
    node: Arc<RwLock<Node>>,
}

impl NodeServer {
    fn new(n: Arc<RwLock<Node>>) -> Self {
        NodeServer{
            node: n,
        }
    }
}

#[tarpc::server]
impl NodeRPC for NodeServer {
    async fn request_vote(self, _: context::Context, e:Election) -> (u32, bool) {
        let mut dv = false;
        if let Ok(mut node) = self.node.write() {
            node.last_rpc_instant = tokio::time::Instant::now();
            if !(matches!(node.state, NodeState::Follower)) {
                return (node.current_term, false);
            }

            match node.deserves_vote(&e) {
                Ok(b) => {
                    dv = b;
                },
                Err(e) => {
                    println!("{}", e);
                },
            };
            if node.current_term < e.term {
               node.current_term = e.term;
            }
        }
        (e.term, dv)
    }

    async fn append_entries(self, _: context::Context, mut ae: AppendEntries) -> (u32, bool) {
        // This is always sent by the leader
        let mut success = false;
        if let Ok(mut node) = self.node.write() {
            node.last_rpc_instant = tokio::time::Instant::now();
            if ae.term < node.current_term {
                return (node.current_term, false);
            }

            #[allow(clippy::if_same_then_else)] // Readability
            if ae.prev_log_index > (node.logs.len() - 1) as u64 {
                return (node.current_term, false);
            } else if ae.prev_log_term != node.logs[ae.prev_log_index as usize].term {
                return (node.current_term, false);
            }

            if let Some(d) = node.logs.get((ae.prev_log_index + 1) as usize) {
                if let Some(x) = ae.entries.get(0) {
                    if d.term > x.term {
                        return (node.current_term, false);
                    }
                }
            }
            success = true;
            let _ = (&mut node.logs).split_off((ae.prev_log_index+1) as usize);
            while let Some(v) = ae.entries.pop() {
                (&mut node.logs).push(v);
            }

            if ae.leader_commit > node.commit_index {
                let mut index = (node.logs.len() - 1) as u64;
                if ae.leader_commit < index {
                    index = ae.leader_commit;
                }
                node.commit_index = index;

                if node.commit_index > node.last_applied {
                    node.last_applied = node.commit_index;
                }
            }

            if ae.term > node.current_term {
                node.current_term = ae.term;
                node.state = NodeState::Follower;
            }
        }
        (ae.term, success)
    }
}

#[derive(Debug, Default)]
struct Cluster {
    members: HashSet<NodeID>,
    leader: NodeID,
    majority: u16,
}

struct NodeClient {
    election_timeout: tokio::time::Duration,
    rpc_timeout: tokio::time::Duration,
    node: Arc<RwLock<Node>>,
    rx: mpsc::Receiver<usize>,
}

impl NodeClient {
    fn new(amn: Arc<RwLock<Node>>, mut rx: mpsc::Receiver<usize> ) -> Self {
        let mut rng = rand::thread_rng();
        let n: u64 = rng.gen_range(250..=500);
        NodeClient {
            election_timeout: tokio::time::Duration::from_millis(n),
            rpc_timeout: tokio::time::Duration::from_millis(20),
            node: amn,
            rx,
        }
    }

    async fn monitor_election(&self) {
        let elec_sleep = time::sleep(self.election_timeout);
        tokio::pin!(elec_sleep);
        loop {
            tokio::select! {
                () = &mut elec_sleep => {
                    println!("timer elapsed");
                    if let Ok(node) = self.node.as_ref().read() {
                        let d = elec_sleep.deadline().duration_since(
                            node.last_rpc_instant,
                            ).as_millis();
                        if d > self.election_timeout.as_millis() {

                        } else {
                            elec_sleep.as_mut().reset(
                                tokio::time::Instant::now()+
                                Duration::from_millis(
                                    (self.election_timeout.as_millis() - d) as u64,
                                ))
                        }
                    }
                },
            }
        }
    }
}

#[derive(Parser,Debug)]
#[clap(author,version,about,long_about=None)]
struct Args {
    #[clap(short,long, default_value = "127.0.0.1")]
    listen: String,

    #[clap(short, long, default_value_t = 8080)]
    port: u16,
}

#[tokio::main]
async fn main() -> Result<()> {
    let args = Args::parse();
    let (ct, st) = tarpc::transport::channel::unbounded();
    let server = server::BaseChannel::with_defaults(st);
    println!("Listening at {}:{}", args.listen, args.port);

    let node = Node::new(args.listen, args.port);
    let amn = Arc::new(RwLock::new(node));
    let (tx,rx) = mpsc::channel(10);
    let ns = NodeServer::new(amn.clone());
    tokio::spawn(server.execute(ns.serve()));

    let nc = NodeClient::new(amn, rx);

    // let cl = NodeRPCClient::new(client::Config::default(), ct).spawn();

    // if (cl.ok(context::current()).await).is_ok() {
    //     println!("health check passed");
    // } else {
    //     println!("health check failed");
    // }

    // println!("{hello}");

    Ok(())
}
