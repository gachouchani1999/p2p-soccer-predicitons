use libp2p::{
    core::upgrade,
    floodsub::{Floodsub, FloodsubEvent, Topic},
    futures::StreamExt,
    identity,
    mdns::{Mdns, MdnsEvent},
    mplex,
    noise::{Keypair, NoiseConfig, X25519Spec},
    swarm::{NetworkBehaviourEventProcess, Swarm, SwarmBuilder},
    tcp::TcpConfig,
    NetworkBehaviour, PeerId, Transport,
};
use log::{error, info};
use once_cell::sync::Lazy;
use serde::{Deserialize, Serialize};
use std::collections::HashSet;
use tokio::{fs, io::AsyncBufReadExt, sync::mpsc};

// Each peer storage location for predictions

const STORAGE_FILE_PATH: &str = "./predictions.json";
// Helper type for Result that lets us propagate generic errors
type Result<T> = std::result::Result<T,Box<dyn std::error::Error + Send + Sync + 'static>>;
// Lazy initialization 
// Key pair ensure uniqueness
static KEYS: Lazy<identity::Keypair> = Lazy::new(|| identity::Keypair::generate_ed25519());
// Unique identifiers for different peers on a network
static PEER_ID: Lazy<PeerId> = Lazy::new(|| PeerId::from(KEYS.public()));
static TOPIC: Lazy<Topic> = Lazy::new(|| Topic::new("predictions"));

type Predictions = Vec<Prediction>;
// Creating the Prediction struct
#[derive(Debug, Serialize, Deserialize)]
struct Prediction {
    id: usize,
    team1: String, 
    team2: String,
    score1: String,
    score2: String,
    // predictions can be private or public
    public: bool
}

// Types of messages to send in p2p
// List mode decides whether to read predictions of one peer or of all peers
#[derive(Debug, Serialize, Deserialize)]
enum ListMode {
    ALL,
    One(String)
}

#[derive(Debug, Serialize, Deserialize)]
struct ListRequest {
    mode: ListMode,
}

#[derive(Debug, Serialize, Deserialize)]
struct ListResponse {
    mode: ListMode,
    data: Predictions,
    receiver: String
}
// Differentiates between response and input of a peer
enum EventType { 
    Response(ListResponse),
    Input(String),
}
#[derive(NetworkBehaviour)]
struct PredictionBehavior {
    floodsub: Floodsub,
    mdns: Mdns,
    #[behaviour(ignore)]
    response_sender: mpsc::UnboundedSender<ListResponse>,
}
#[tokio::main]
async fn main() {
    pretty_env_logger::init();
    info!("Peer Id: {}  ", PEER_ID.clone());
    // Create async channel to send responses to our app.
    let (response_sender, mut response_rcv) = mpsc::unbounded_channel();

    let auth_keys = Keypair::<X25519Spec>::new()
        .into_authentic(&KEYS)
        .expect("can create auth keys");
        // Create a transport to allow communication in a p2p network
        let transp = TcpConfig::new().upgrade(upgrade::Version::V1).authenticate(NoiseConfig::xx(auth_keys)
        .into_authenticated())
        .multiplex(mplex::MplexConfig::new())
        .boxed();

    let mut behavior = PredictionBehavior {
        floodsub: Floodsub::new(PEER_ID.clone()),
        mdns: Mdns::new(Default::default()).await.expect("can create mdns"),
        response_sender
    };
    // Uses mDNS since we want to find peers in a local network
    behavior.floodsub.subscribe(TOPIC.clone());

    // Swarm to manage the connections 
    let mut swarm = SwarmBuilder::new(transp, behavior, PEER_ID.clone())
    .executor(Box::new(|fut| {
        tokio::spawn(fut);
    }))
    .build();
    // Reads stream line by line
    let mut stdin = tokio::io::BufReader::new(tokio::io::stdin()).lines();

    Swarm::listen_on(
        &mut swarm,
        "/ip4/0.0.0.0/tcp/0"
            .parse()
            .expect("can get a local socket"),
    )
    .expect("swarm can be started");

    // create loop that listens to STD events and from Swarm
    loop {
        let evt = {
            tokio::select! {
                line = stdin.next_line() => Some(EventType::Input(line.expect("can get line").expect("can read line from stdin"))),
                response = response_rcv.recv() => Some(EventType::Response(response.expect("response exists"))),
                event = swarm.select_next_some() => {
                    info!("Unhandled Swarm Event: {:?}", event);
                    None
                },
            }
        };

        if let Some(event) = evt {
            match event {
                EventType::Response(resp) => {
                    let json = serde_json::to_string(&resp).expect("can jsonify response");
                    swarm.behaviour_mut().floodsub.publish(TOPIC.clone(), json.as_bytes());
                }
                // Check if event response or input
                EventType::Input(line) => match line.as_str() {
                    // list all peers in the network
                    "ls p" => handle_list_peers(&mut swarm).await,
                    // list all predictions in the network
                    cmd if cmd.starts_with("ls pr") => handle_list_predictions(cmd, &mut swarm).await,
                    // handles creating a new prediction
                    cmd if cmd.starts_with("create pr") => handle_create_prediction(cmd).await,
                    // publishes a predictions
                    cmd if cmd.starts_with("publish pr") => handle_publish_prediction(cmd).await,
                    _ => error!("unknown command"),
                },
                
            }
        }
    }
}
// use mDNS to list all nodes
async fn handle_list_peers(swarm: &mut Swarm<PredictionBehavior>) {
    info!("Discovered peers: ");
    let nodes = swarm.behaviour().mdns.discovered_nodes();
    let mut unique_peers = HashSet::new();
    for peer in  nodes {
        unique_peers.insert(peer);
    }
    unique_peers.iter().for_each(|p| info!("{}",p));
}

async fn handle_create_prediction(cmd: &str) { 
    if let Some(rest) = cmd.strip_prefix("create pr") {
        let elements: Vec<&str> = rest.split("|").collect();
        if elements.len() < 3 {
            info!("too few arguments - Format: team1|team2|score1|score2");
        } else {
            let team_1 = elements.get(1).expect("team 1 is there");
            let team_2 = elements.get(2).expect("team 2 is there");
            let score_1 = elements.get(3).expect("score 1 is there");
            let score_2 = elements.get(4).expect("score 2 is there");

            if let Err(e) = create_new_prediction(team_1, team_2, score_1, score_2).await {
                error!("error creating prediction: {}", e);
            };
        }
    }
}


async fn handle_publish_prediction(cmd: &str) {
    if let Some(rest) = cmd.strip_prefix("publish pr") {
        match rest.trim().parse::<usize>() {
            Ok(id) => {
                if let Err(e) = publish_prediction(id).await {
                    info!("error publishing prediction with id {}, {}", id, e)
                } else {
                    info!("Published Prediction with id: {}", id);
                }
            }
            Err(e) => error!("invalid id: {}, {}", rest.trim(), e),
        };
    }
}

async fn create_new_prediction(team_1: &str, team_2: &str, score_1: &str, score_2: &str) -> Result<()> {
    // Read local peer predictions
    let mut local_predictions = read_local_predictions().await?;
    let new_id = match local_predictions.iter().max_by_key(|pr| pr.id) {
        Some(v) => v.id + 1,
        None => 0,
    };
    local_predictions.push(Prediction {
        id: new_id,
        team1: team_1.to_owned(),
        team2: team_2.to_owned(),
        score1: score_1.to_owned(),
        score2: score_2.to_owned(),
        public: false,
    });

    write_local_predictions(&local_predictions).await?;

    info!("Created prediciton:");
    info!("Team1: {}", team_1);
    info!("Team2: {}", team_2);
    info!("Score1: {}", score_1);
    info!("Score2: {}", score_2);


    Ok(())
}

async fn publish_prediction(id: usize) -> Result<()> {
    let mut local_predictions = read_local_predictions().await?;
    local_predictions
        .iter_mut()
        .filter(|pr| pr.id == id)
        .for_each(|pr| pr.public = true);
    write_local_predictions(&local_predictions).await?;
    Ok(())
}
// Read and deserialize from storage
async fn read_local_predictions() -> Result<Predictions> {
    let content = fs::read(STORAGE_FILE_PATH).await?;
    let result = serde_json::from_slice(&content)?;
    Ok(result)
}
// Write and serialize to storage
async fn write_local_predictions(predictions: &Predictions) -> Result<()> {
    let json = serde_json::to_string(&predictions)?;
    fs::write(STORAGE_FILE_PATH, &json).await?;
    Ok(())
}

async fn handle_list_predictions(cmd: &str, swarm: &mut Swarm<PredictionBehavior>) {
    let rest = cmd.strip_prefix("ls pr");
    match rest {
        Some("all") => {
            let req = ListRequest {
                mode: ListMode::ALL,
            };
            let json = serde_json::to_string(&req).expect("can jsonify request");
            swarm.behaviour_mut().floodsub.publish(TOPIC.clone(), json.as_bytes());
        }
        Some(recipes_peer_id) => {
            let req = ListRequest {
                mode: ListMode::One(recipes_peer_id.to_owned()),
            };
            let json = serde_json::to_string(&req).expect("can jsonify request");
            swarm.behaviour_mut().floodsub.publish(TOPIC.clone(), json.as_bytes());
        }
        None => {
            match read_local_predictions().await {
                Ok(v) => {
                    info!("Local Predictions ({})", v.len());
                    v.iter().for_each(|r| info!("{:?}", r));
                }
                Err(e) => error!("error fetching local predictions: {}", e),
            };
        }
    };
}
impl NetworkBehaviourEventProcess<FloodsubEvent> for PredictionBehavior {
    fn inject_event(&mut self, event: FloodsubEvent) {
        match event {
            FloodsubEvent::Message(msg) => {
                if let Ok(resp) = serde_json::from_slice::<ListResponse>(&msg.data) {
                    if resp.receiver == PEER_ID.to_string() {
                        info!("Response from {}:", msg.source);
                        resp.data.iter().for_each(|r| info!("{:?}", r));
                    }
                } else if let Ok(req) = serde_json::from_slice::<ListRequest>(&msg.data) {
                    match req.mode {
                        ListMode::ALL => {
                            info!("Received ALL req: {:?} from {:?}", req, msg.source);
                            respond_with_public_predictions(
                                self.response_sender.clone(),
                                msg.source.to_string(),
                            );
                        }
                        ListMode::One(ref peer_id) => {
                            if peer_id == &PEER_ID.to_string() {
                                info!("Received req: {:?} from {:?}", req, msg.source);
                                respond_with_public_predictions(
                                    self.response_sender.clone(),
                                    msg.source.to_string(),
                                );
                            }
                        }
                    }
                }
            }
            _ => (),
        }
    }
}
impl NetworkBehaviourEventProcess<MdnsEvent> for PredictionBehavior {
    fn inject_event(&mut self, event: MdnsEvent) {
        match event {
            MdnsEvent::Discovered(discovered_list) => {
                for (peer, _addr) in discovered_list {
                    self.floodsub.add_node_to_partial_view(peer);
                }
            }
            MdnsEvent::Expired(expired_list) => {
                for (peer, _addr) in expired_list {
                    if !self.mdns.has_node(&peer) {
                        self.floodsub.remove_node_from_partial_view(&peer);
                    }
                }
            }
        }
    }
}

fn respond_with_public_predictions(sender: mpsc::UnboundedSender<ListResponse>, receiver: String) {
    tokio::spawn(async move {
        match read_local_predictions().await {
            Ok(predictions) => {
                let resp = ListResponse {
                    mode: ListMode::ALL,
                    receiver,
                    data: predictions.into_iter().filter(|r| r.public).collect(),
                };
                if let Err(e) = sender.send(resp) {
                    error!("error sending response via channel, {}", e);
                }
            }
            Err(e) => error!("error fetching local predictions to answer ALL request, {}", e),
        }
    });
}





