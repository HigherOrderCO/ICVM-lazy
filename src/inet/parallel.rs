use std::{sync::{atomic::{AtomicU32, Ordering}, RwLock}, ops::{Index, IndexMut}};
use crate::term::definition_book::DefinitionBook;
use flume::{Sender, unbounded, Receiver};
use rayon::{prelude::*, Scope};
use super::{NodeId, ROOT, port, Port, slot, addr, NodeKind, TAG_MASK, REF};

/// Represents active pair of nodes in a net, meaning that their principal ports are connected.
/// Not necessarily that a rewrite rule exists for this pair.
pub type ActiveNodePair = (NodeId, NodeId);

pub type ActiveNodePairs = Vec<ActiveNodePair>;

type Cell = u32;

const UNUSED_CELL: Cell = Cell::MAX;

const CELLS_PER_NODE: usize = 4;

#[derive(Debug)]
struct Node([Cell; CELLS_PER_NODE]);

type Foo = RwLock<Node>;

// Parallel interaction combinator net reduction
#[derive(Debug)]
pub struct ParINet {
  nodes: Box<[RwLock<Node>]>,
  tx_free_nodes: Sender<NodeId>,
  rx_free_nodes: Receiver<NodeId>,
}

impl ParINet {
  // Create a new net, with a deadlocked root node.
  pub fn new() -> Self {
    let (tx_free_nodes, rx_free_nodes) = unbounded();
    // p2 points to p0, p1 points to net
    Self {
      nodes: vec![2,1,0,0],
      tx_free_nodes,
      rx_free_nodes,
    }
  }

  // Allocates a new node, reclaiming a freed space if possible.
  pub fn new_node(&self, kind: NodeKind) -> NodeId {
    let node_id = self.rx_free_nodes.recv().unwrap();

    debug_assert_eq!(self.nodes[port(node_id, 0) as usize].load(Ordering::SeqCst), UNUSED_CELL);
    debug_assert_eq!(self.nodes[port(node_id, 1) as usize].load(Ordering::SeqCst), UNUSED_CELL);
    debug_assert_eq!(self.nodes[port(node_id, 2) as usize].load(Ordering::SeqCst), UNUSED_CELL);
    debug_assert_eq!(self.nodes[port(node_id, 3) as usize].load(Ordering::SeqCst), UNUSED_CELL);

    self.nodes[port(node_id, 0) as usize].store(port(node_id, 0), Ordering::SeqCst);
    self.nodes[port(node_id, 1) as usize].store(port(node_id, 1), Ordering::SeqCst);
    self.nodes[port(node_id, 2) as usize].store(port(node_id, 2), Ordering::SeqCst);
    self.nodes[port(node_id, 3) as usize].store(kind, Ordering::SeqCst);

    node_id
  }

  fn free_node(&self, node_id: NodeId) {
    self.nodes[node_id as usize].write();
    self.tx_free_nodes.send(node_id).unwrap();
  }

  // Links two ports.
  fn link(&self, ptr_a: Port, ptr_b: Port) {
    self.nodes[ptr_a as usize].store(ptr_b, Ordering::SeqCst);
    self.nodes[ptr_b as usize].store(ptr_a, Ordering::SeqCst);
  }

  // Enters a port, returning the port on the other side.
  pub fn enter(&self, port: Port) -> Port {
    self.nodes[port as usize].load(Ordering::SeqCst)
  }

  // Kind of the node.
  pub fn kind(&self, node: NodeId) -> NodeKind {
    self.nodes[port(node, 3) as usize].load(Ordering::SeqCst)
  }

  /// Determines if a given node is part of an active pair and returns the other node in the pair
  #[inline(always)]
  fn node_is_part_of_active_pair(&self, node_id: NodeId) -> Option<NodeId> {
    let dst = self.enter(port(node_id, 0));
    (slot(dst) == 0).then(|| addr(dst))
  }

  /// Determine active pairs that can potentially be rewritten if there is a matching rule
  pub fn scan_active_pairs(&self) -> ActiveNodePairs {
    let mut active_pairs = vec![];
    for (node_id, node) in self.nodes.iter().enumerate() {
      if node.load(Ordering::SeqCst) == UNUSED_CELL {
        continue;
      }

      let node_id = node_id as NodeId;
      if let Some(dst_node_idx) = self.node_is_part_of_active_pair(node_id) {
        // Only process each bidirectional link once, to prevent duplicates
        if node_id < dst_node_idx {
          active_pairs.push((node_id, dst_node_idx));
        }
      }
    }
    active_pairs
  }

  pub fn normalize(&self, definition_book: &DefinitionBook) {
    let active_pairs = self.scan_active_pairs();
    rayon::scope(|s| {
      fn process_active_pair<'scope, 'net: 'scope>(
        s: &'scope Scope<'net>,
        net: &'net ParINet,
        active_pair: ActiveNodePair,
        definition_book: &'net DefinitionBook,
      ) {
        for new_active_pair in net.rewrite(active_pair, definition_book) {
          s.spawn(move |s| {
            process_active_pair(s, net, new_active_pair, definition_book);
          });
        }
      }
      for active_pair in active_pairs {
        s.spawn(move |s| {
          process_active_pair(s, self, active_pair, definition_book);
        });
      }
    });
  }

  // fn rewrite(&self, active_pair: ActiveNodePair, definition_book: &DefinitionBook) -> ActiveNodePairs {
  fn rewrite(&self, (x, y): ActiveNodePair, definition_book: &DefinitionBook) -> impl IntoIterator<Item = ActiveNodePair> {
    debug_assert_eq!(self.enter(port(x, 0)), port(y, 0));
    debug_assert_eq!(self.enter(port(y, 0)), port(x, 0));

    let kind_x = self.kind(x);
    let kind_y = self.kind(y);

    if kind_x & TAG_MASK == REF || kind_y & TAG_MASK == REF {
      todo!();
    }

    if kind_x == kind_y {
      let p0 = self.enter(port(x, 1));
      let p1 = self.enter(port(y, 1));
      self.link(p0, p1);
      let p0 = self.enter(port(x, 2));
      let p1 = self.enter(port(y, 2));
      self.link(p0, p1);
      self.free_node(x);
      self.free_node(y);
    } else {
      let a = self.new_node(self.kind(x));
      let b = self.new_node(self.kind(y));
      self.link(port(b, 0), self.enter(port(x, 1)));
      self.link(port(y, 0), self.enter(port(x, 2)));
      self.link(port(a, 0), self.enter(port(y, 1)));
      self.link(port(x, 0), self.enter(port(y, 2)));
      self.link(port(a, 1), port(b, 1));
      self.link(port(a, 2), port(y, 1));
      self.link(port(x, 1), port(b, 2));
      self.link(port(x, 2), port(y, 2));
    }
    []
  }
}
