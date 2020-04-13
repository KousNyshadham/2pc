//! 
//! client.rs
//! Implementation of 2PC client
//! 
extern crate log;
extern crate stderrlog;
use std::sync::mpsc::{Sender, Receiver};
use std::sync::atomic::{AtomicI32, AtomicBool, Ordering};
use std::sync::{Arc, Mutex};
use std::time::Duration;
use std::thread;
use std::collections::HashMap;
use message;
use message::MessageType;
use message::ProtocolMessage;
use message::RequestStatus;

// static counter for getting unique TXID numbers
static TXID_COUNTER: AtomicI32 = AtomicI32::new(1);

// client state and 
// primitives for communicating with 
// the coordinator
#[derive(Debug)]
pub struct Client {    
    pub id: i32,
	pub is: String,
    pub txl: Arc<Mutex<Sender<ProtocolMessage>>>, 
    pub rxl: Arc<Mutex<Receiver<ProtocolMessage>>>, 
	pub r: Arc<AtomicBool>,
	pub ccommit: i32,
	pub cabort: i32,
}

///
/// client implementation
/// Required: 
/// 1. new -- ctor
/// 2. pub fn report_status -- reports number of committed/aborted/unknown 
/// 3. pub fn protocol(&mut self, n_requests: i32) -- implements client side protocol
///
impl Client {

    /// 
    /// new()
    /// 
    /// Return a new client, ready to run the 2PC protocol
    /// with the coordinator. 
    /// 
    /// HINT: you may want to pass some channels or other communication 
    ///       objects that enable coordinator->client and client->coordinator
    ///       messaging to this ctor.
    /// HINT: you may want to pass some global flags that indicate whether
    ///       the protocol is still running to this constructor
    /// 
    pub fn new(i: i32,
               is: String,
    			txl: Arc<Mutex<Sender<ProtocolMessage>>>, 
    			rxl: Arc<Mutex<Receiver<ProtocolMessage>>>, 
               r: Arc<AtomicBool>) -> Client {
        Client {
            id: i,
			is,
			txl,
			rxl,
			r,
			ccommit:0,
			cabort:0,
        }   
    }

    ///
    /// wait_for_exit_signal(&mut self)
    /// wait until the running flag is set by the CTRL-C handler
    /// 
    pub fn wait_for_exit_signal(&mut self) {

        trace!("Client_{} waiting for exit signal", self.id);

		while self.r.load(Ordering::SeqCst){
		}

        trace!("Client_{} exiting", self.id);
    }

    /// 
    /// send_next_operation(&mut self)
    /// send the next operation to the coordinator
    /// 
    pub fn send_next_operation(&mut self) {

        trace!("Client_{}::send_next_operation", self.id);

        // create a new request with a unique TXID.         
        //let request_no: i32 = 0; // TODO--choose another number!
        let txid = TXID_COUNTER.fetch_add(1, Ordering::SeqCst);
		let request_no = txid;


        info!("Client {} request({})->txid:{} called", self.id, request_no, txid);
        let pm = message::ProtocolMessage::generate(message::MessageType::ClientRequest, 
                                                    txid, 
                                                    format!("Client_{}", self.id),self.id,
                                                    request_no);

        info!("client {} calling send...", self.id);

		let	tx = self.txl.lock().unwrap();
		tx.send(pm).unwrap();

        trace!("Client_{}::exit send_next_operation", self.id);
    }

    ///
    /// recv_result()
    /// Wait for the coordinator to respond with the result for the 
    /// last issued request. Note that we assume the coordinator does 
    /// not fail in this simulation
    /// 
    pub fn recv_result(&mut self) {

        trace!("Client_{}::recv_result", self.id);
		let rx = self.rxl.lock().unwrap();
		let msg = rx.recv().unwrap();
		if(msg.mtype == MessageType::ClientResultCommit){
			self.ccommit+=1;
		}
		else{
			self.cabort+=1;
		}

        trace!("Client_{}::exit recv_result", self.id);
    }

    ///
    /// report_status()
    /// report the abort/commit/unknown status (aggregate) of all 
    /// transaction requests made by this client before exiting. 
    /// 
    pub fn report_status(&mut self) {

        // TODO: collect real stats!
        let successful_ops: i32 = self.ccommit;
        let failed_ops: i32 = self.cabort;
        let unknown_ops: i32 = 0; 
        println!("Client_{}:\tC:{}\tA:{}\tU:{}", self.id, successful_ops, failed_ops, unknown_ops);
    }    

    ///
    /// protocol()
    /// Implements the client side of the 2PC protocol
    /// HINT: if the simulation ends early, don't keep issuing requests!
    /// HINT: if you've issued all your requests, wait for some kind of
    ///       exit signal before returning from the protocol method!
    /// 
    pub fn protocol(&mut self, n_requests: i32) {

        // run the 2PC protocol for each of n_requests

		for i in 0..n_requests{
			self.send_next_operation();
			self.recv_result();
		}
		

		

        // wait for signal to exit
        // and then report status
        self.wait_for_exit_signal();
		if(self.id == 0){
				let pm = message::ProtocolMessage::generate(message::MessageType::CoordinatorExit, 
															69, 
															format!("Client_{}", 69),69,
															69);

				let	tx = self.txl.lock().unwrap();
				tx.send(pm).unwrap();

		}
        self.report_status();
    }
}
