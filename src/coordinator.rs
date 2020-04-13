//! 
//! coordinator.rs
//! Implementation of 2PC coordinator
//! 
extern crate log;
extern crate stderrlog;
extern crate rand;
use coordinator::rand::prelude::*;use std::thread;
use std::sync::{Arc};
use std::sync::Mutex;
use std::sync::mpsc;
use std::sync::mpsc::channel;
use std::sync::mpsc::{Sender, Receiver};
use std::time::Duration;
use std::collections::HashMap;
use std::sync::atomic::{AtomicI32};
use std::sync::atomic::{AtomicBool, Ordering};
use message::ProtocolMessage;
use message::MessageType;
use message::RequestStatus;
use message;
use oplog;


/// Coordinator
/// struct maintaining state for coordinator
#[derive(Debug)]
pub struct Coordinator {
	r: Arc<AtomicBool>,
    log: oplog::OpLog,
    msg_success_prob: f64,
	pub tx: Sender<message::ProtocolMessage>,
	pub rx: Receiver<message::ProtocolMessage>,
	pub tx2: Sender<message::ProtocolMessage>,
	pub rx2: Receiver<message::ProtocolMessage>,
	ccommit: i32,
	cabort: i32,
	client_txs: Vec<Sender<message::ProtocolMessage>>,
	participant_txs: Vec<Sender<message::ProtocolMessage>>,
}

///
/// Coordinator
/// implementation of coordinator functionality
/// Required:
/// 1. new -- ctor
/// 2. protocol -- implementation of coordinator side of protocol
/// 3. report_status -- report of aggregate commit/abort/unknown stats on exit.
/// 4. participant_join -- what to do when a participant joins
/// 5. client_join -- what to do when a client joins
/// 
impl Coordinator {

    ///
    /// new()
    /// Initialize a new coordinator
    /// 
    /// <params>
    ///     logpath: directory for log files --> create a new log there. 
    ///     r: atomic bool --> still running?
    ///     msg_success_prob --> probability sends succeed
    ///
    pub fn new(
        logpath: String, 
        r: Arc<AtomicBool>, 
        msg_success_prob: f64, tx: Sender<message::ProtocolMessage>, rx: Receiver<message::ProtocolMessage>, tx2:Sender<message::ProtocolMessage>, rx2: Receiver<message::ProtocolMessage>) -> Coordinator {

        Coordinator {
			rx,
			tx,
			rx2,
			tx2,
			ccommit:0,
			cabort:0,
            log: oplog::OpLog::new(logpath),
            msg_success_prob: msg_success_prob,
			r,
			client_txs: Vec::new(),
			participant_txs:  Vec::new(),

        }
    }

    /// 
    /// participant_join()
    /// handle the addition of a new participant
    /// HINT: keep track of any channels involved!
    /// HINT: you'll probably need to change this routine's 
    ///       signature to return something!
    ///       (e.g. channel(s) to be used)
    /// 
    //pub fn participant_join(&mut self, name: &String) {
    pub fn participant_join(&mut self, participant_tx: Sender<message::ProtocolMessage>) {

		self.participant_txs.push(participant_tx);
    }

    /// 
    /// client_join()
    /// handle the addition of a new client
    /// HINTS: keep track of any channels involved!
    /// HINT: you'll probably need to change this routine's 
    ///       signature to return something!
    ///       (e.g. channel(s) to be used)
    /// 
    //pub fn client_join(&mut self, name: &String)  {
    pub fn client_join(&mut self, client_tx: Sender<message::ProtocolMessage>)  {

		self.client_txs.push(client_tx);	
    }

    /// 
    /// send()
    /// send a message, maybe drop it
    /// HINT: you'll need to do something to implement 
    ///       the actual sending!
    /// 
    pub fn send(&mut self, sender: &Sender<ProtocolMessage>, pm: ProtocolMessage) -> bool {

        let x: f64 = random();
        let mut result: bool = true;
		sender.send(pm).unwrap();
        result
    }     

    /// 
    /// recv_request()
    /// receive a message from a client
    /// to start off the protocol.
    /// 
    pub fn recv_request(&mut self) -> ProtocolMessage {

        trace!("coordinator::recv_request...");

		let msg = self.rx.recv().unwrap();
		


        trace!("leaving coordinator::recv_request");
        msg
    }        

    ///
    /// report_status()
    /// report the abort/commit/unknown status (aggregate) of all 
    /// transaction requests made by this coordinator before exiting. 
    /// 
    pub fn report_status(&mut self) {
        let successful_ops: i32 = self.ccommit;
        let failed_ops: i32 = self.cabort; 
        let unknown_ops: usize = 0; 
        println!("coordinator:\tC:{}\tA:{}\tU:{}", successful_ops, failed_ops, unknown_ops);
    }    

    ///
    /// protocol()
    /// Implements the coordinator side of the 2PC protocol
    /// HINT: if the simulation ends early, don't keep handling requests!
    /// HINT: wait for some kind of exit signal before returning from the protocol!
    /// 
    pub fn protocol(&mut self) {

		while(true){
			let msg = self.recv_request();
			let mut count = 0;
			for participant_tx in self.participant_txs.clone(){
				let mut two_participant = ProtocolMessage::generate(MessageType::CoordinatorPropose, msg.txid,msg.senderstringid.clone(), msg.senderid,msg.opid);
				if(msg.mtype == MessageType::CoordinatorExit){
					two_participant = ProtocolMessage::generate(MessageType::CoordinatorExit, msg.txid,msg.senderstringid.clone(),msg.senderid,msg.opid);
				}
				self.send(&participant_tx, two_participant);
				if(msg.mtype != MessageType::CoordinatorExit){
					let msg2 = self.rx2.recv().unwrap();
					if(msg2.mtype == MessageType::ParticipantVoteAbort){
						count+=1;
					}
				}
			}
			if(msg.mtype == MessageType::CoordinatorExit){
				break;
			}
			for participant_tx in self.participant_txs.clone(){
				let mut sendmsg = ProtocolMessage::generate(MessageType::CoordinatorCommit,msg.txid,msg.senderstringid.clone(),msg.senderid,msg.opid);
				if(count >= 1){
					sendmsg = ProtocolMessage::generate(MessageType::CoordinatorAbort, msg.txid,msg.senderstringid.clone(),msg.senderid,msg.opid);
				}
				self.send(&participant_tx, sendmsg);
			}
			let mut sendmsg = ProtocolMessage::generate(MessageType::ClientResultCommit,msg.txid,msg.senderstringid.clone(),msg.senderid,msg.opid);
			if(count >= 1){
				sendmsg = ProtocolMessage::generate(MessageType::ClientResultAbort, msg.txid,msg.senderstringid.clone(),msg.senderid,msg.opid);
			}
			if(count>=1){
				self.log.append(MessageType::CoordinatorAbort, msg.txid,msg.senderstringid.clone(), msg.senderid,msg.opid);
				self.cabort +=1;
			}
			else{
				self.log.append(MessageType::CoordinatorCommit, msg.txid,msg.senderstringid.clone(), msg.senderid,msg.opid);
				self.ccommit +=1;
			}
			let index :usize = msg.senderid as usize;
			let client = self.client_txs[index].clone();
			self.send(&client,sendmsg);
		}
        self.report_status();                        
    }
}
