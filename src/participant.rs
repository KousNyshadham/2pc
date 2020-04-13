//! 
//! participant.rs
//! Implementation of 2PC participant
//! 
extern crate log;
extern crate stderrlog;
extern crate rand;
use participant::rand::prelude::*;
use std::sync::mpsc;
use std::sync::mpsc::{Sender, Receiver};
use std::time::Duration;
use std::sync::atomic::{AtomicI32};
use std::sync::{Arc, Mutex};
use std::sync::atomic::{AtomicBool, Ordering};
use message::MessageType;
use message::ProtocolMessage;
use message::RequestStatus;
use std::collections::HashMap;
use std::thread;
use oplog;


///
/// Participant
/// structure for maintaining per-participant state 
/// and communication/synchronization objects to/from coordinator
/// 
#[derive(Debug)]
pub struct Participant {    
    id: i32,
	is: String,
    log: oplog::OpLog,
    op_success_prob: f64,
    msg_success_prob: f64,
	r: Arc<AtomicBool>,
    txl: Arc<Mutex<Sender<ProtocolMessage>>>, 
    rxl: Arc<Mutex<Receiver<ProtocolMessage>>>, 
	pcommit:i32,
	pabort:i32,
}

///
/// Participant
/// implementation of per-participant 2PC protocol
/// Required:
/// 1. new -- ctor
/// 2. pub fn report_status -- reports number of committed/aborted/unknown for each participant
/// 3. pub fn protocol() -- implements participant side protocol
///
impl Participant {

    /// 
    /// new()
    /// 
    /// Return a new participant, ready to run the 2PC protocol
    /// with the coordinator. 
    /// 
    /// HINT: you may want to pass some channels or other communication 
    ///       objects that enable coordinator->participant and participant->coordinator
    ///       messaging to this ctor.
    /// HINT: you may want to pass some global flags that indicate whether
    ///       the protocol is still running to this constructor. There are other
    ///       ways to communicate this, of course. 
    /// 
    pub fn new(
        i: i32, is: String, 
        txl: Arc<Mutex<Sender<ProtocolMessage>>>, 
        rxl: Arc<Mutex<Receiver<ProtocolMessage>>>, 
        logpath: String,
        r: Arc<AtomicBool>,
        f_success_prob_ops: f64,
        f_success_prob_msg: f64) -> Participant {

        Participant {
            id: i,
			is,
            log: oplog::OpLog::new(logpath),
            op_success_prob: f_success_prob_ops,
            msg_success_prob: f_success_prob_msg,
			r,
			txl,
			rxl,
			pcommit:0,
			pabort:0,
        }   
    }

    ///
    /// send()
    /// Send a protocol message to the coordinator.
    /// This variant can be assumed to always succeed.
    /// You should make sure your solution works using this 
    /// variant before working with the send_unreliable variant.
    /// 
    /// HINT: you will need to implement something that does the 
    ///       actual sending.
    /// 
    pub fn send(&mut self, pm: ProtocolMessage) -> bool {
        let result: bool = true;

		let tx = self.txl.lock().unwrap();
		tx.send(pm).unwrap();

        result
    }

    ///
    /// send()
    /// Send a protocol message to the coordinator, 
    /// with some probability of success thresholded by the 
    /// command line option success_probability [0.0..1.0].
    /// This variant can be assumed to always succeed
    /// 
    /// HINT: you will need to implement something that does the 
    ///       actual sending, but you can use the threshold 
    ///       logic in this implementation below. 
    /// 
    pub fn send_unreliable(&mut self, msg: ProtocolMessage) -> bool {
        let x: f64 = random();
        let result: bool;
        if x < self.msg_success_prob {
            result = self.send(msg);
        } else {
			let mut fail = ProtocolMessage::generate(MessageType::ParticipantVoteAbort, msg.txid,msg.senderstringid.clone(),msg.senderid,msg.opid);
            result = self.send(fail);
        }
        result
    }    

    /// 
    /// perform_operation
    /// perform the operation specified in the 2PC proposal,
    /// with some probability of success/failure determined by the 
    /// command-line option success_probability. 
    /// 
    /// HINT: The code provided here is not complete--it provides some
    ///       tracing infrastructure and the probability logic. 
    ///       Your implementation need not preserve the method signature
    ///       (it's ok to add parameters or return something other than 
    ///       bool if it's more convenient for your design).
    /// 
    pub fn perform_operation(&mut self) -> bool {

        trace!("participant::perform_operation");

        let mut result: RequestStatus = RequestStatus::Unknown;

        let x: f64 = random();
        if x > self.op_success_prob {
			result = RequestStatus::Aborted;

        } else {
			result = RequestStatus::Committed;
        }

        trace!("exit participant::perform_operation");
        result == RequestStatus::Committed
    }

    ///
    /// report_status()
    /// report the abort/commit/unknown status (aggregate) of all 
    /// transaction requests made by this coordinator before exiting. 
    /// 
    pub fn report_status(&mut self) {

        // TODO: maintain actual stats!
        let global_successful_ops: i32 = self.pcommit;
        let global_failed_ops: i32 = self.pabort;
        let global_unknown_ops: usize = 0;
        println!("participant_{}:\tC:{}\tA:{}\tU:{}", self.id, global_successful_ops, global_failed_ops, global_unknown_ops);
    }

    pub fn recv_msg(&mut self) -> ProtocolMessage {

        trace!("coordinator::recv_request...");

		let rx = self.rxl.lock().unwrap();
		let msg = rx.recv().unwrap();
        trace!("leaving coordinator::recv_request");
        msg
    }        

    ///
    /// protocol()
    /// Implements the participant side of the 2PC protocol
    /// HINT: if the simulation ends early, don't keep handling requests!
    /// HINT: wait for some kind of exit signal before returning from the protocol!
    /// 
    pub fn protocol(&mut self) {
        
        trace!("Participant_{}::protocol", self.id);

		while(true){
			let msg = self.recv_msg();
			if(msg.mtype == MessageType::CoordinatorExit){
				break;
			}
			let mut stag1 = ProtocolMessage::generate(MessageType::ParticipantVoteCommit, msg.txid,msg.senderstringid.clone(),msg.senderid,msg.opid);
			let succ = self.perform_operation();
			if(succ == false){
				stag1 = ProtocolMessage::generate(MessageType::ParticipantVoteAbort, msg.txid, msg.senderstringid.clone(), msg.senderid, msg.opid);	
			}
			self.send_unreliable(stag1);
			let globalDecision = self.recv_msg();;
			if(globalDecision.mtype == MessageType::CoordinatorCommit){
				self.pcommit+=1;
				self.log.append(MessageType::CoordinatorCommit, msg.txid,msg.senderstringid.clone(), msg.senderid,msg.opid);
					
			}
			else{
				self.pabort+=1;
				self.log.append(MessageType::CoordinatorAbort, msg.txid,msg.senderstringid.clone(), msg.senderid,msg.opid);
			}
		}

        self.report_status();
    }
}
