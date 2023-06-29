#![allow(dead_code)]

mod as_net;
pub mod definition_book;
mod syntax;
mod views;

pub use self::as_net::*;
use self::definition_book::DefinitionBook;
use self::definition_book::DefinitionId;
use self::definition_book::DefinitionName;
pub use self::syntax::*;
pub use self::views::*;

use std::collections::*;
use inet::*;
use std;

// Terms of the Interaction Calculus.
#[derive(Clone, Debug)]
pub enum Term {
  // Abstractions
  Lam {nam: Vec<u8>, typ: Option<Box<Term>>, bod: Box<Term>},

  // Applications
  App {fun: Box<Term>, arg: Box<Term>},

  // Superpositions
  Sup {tag: u32, fst: Box<Term>, snd: Box<Term>},

  // Duplications
  Dup {tag: u32, fst: Vec<u8>, snd: Vec<u8>, val: Box<Term>, nxt: Box<Term>},

  // Recursion
  Fix {nam: Vec<u8>, bod: Box<Term>},

  // Annotations
  Ann {val: Box<Term>, typ: Box<Term>},

  // Variables
  Var {nam: Vec<u8>},

  // Erasure
  Set
}

use self::Term::{*};

// Source code is Ascii-encoded.
pub type Str = [u8];
pub type Chr = u8;

// Converts an index to a name
pub fn index_to_name(idx : u32) -> Vec<Chr> {
  let mut name = Vec::new();
  let mut idx = idx;
  while idx > 0 {
    idx = idx - 1;
    name.push((97 + idx % 26) as u8);
    idx = idx / 26;
  }
  return name;
}

// Converts a name to an index
pub fn name_to_index(name : &Vec<Chr>) -> u32 {
  let mut idx : u32 = 0;
  for byte in name.iter().rev() {
    idx = (idx * 26) + (*byte as u32 - 97) + 1;
  }
  return idx;
}

// A context is a vector of (name, value) assignments.
type Context<'a> = Vec<(&'a Str, Option<Term>)>;

// Extends a context with a (name, value) assignments.
fn extend<'a,'b>(nam : &'a Str, val : Option<Term>, ctx : &'b mut Context<'a>) -> &'b mut Context<'a> {
  ctx.push((nam,val));
  ctx
}

// Removes an assignment from a context.
fn narrow<'a,'b>(ctx : &'b mut Context<'a>) -> &'b mut Context<'a> {
  ctx.pop();
  ctx
}

pub fn namespace(space : &Vec<u8>, idx : u32, var : &Vec<u8>) -> Vec<u8> {
  if var != b"*" {
    let mut nam = space.clone();
    nam.extend_from_slice(b"/");
    nam.append(&mut idx.to_string().as_bytes().to_vec());
    nam.extend_from_slice(b"/");
    nam.append(&mut var.clone());
    nam
  } else {
    var.clone()
  }
}

// Makes a namespaced copy of a term
pub fn copy(space : &Vec<u8>, idx : u32, term : &Term) -> Term {
  match term {
    Lam{nam, typ, bod} => {
      let nam = namespace(space, idx, nam);
      let typ = typ.as_ref().map(|typ| Box::new(copy(space, idx, typ)));
      let bod = Box::new(copy(space, idx, bod));
      Lam{nam, typ, bod}
    },
    App{fun, arg} => {
      let fun = Box::new(copy(space, idx, fun));
      let arg = Box::new(copy(space, idx, arg));
      App{fun, arg}
    },
    Sup{tag, fst, snd} => {
      let tag = *tag;
      let fst = Box::new(copy(space, idx, fst));
      let snd = Box::new(copy(space, idx, snd));
      Sup{tag, fst, snd}
    },
    Dup{tag, fst, snd, val, nxt} => {
      let tag = *tag;
      let fst = namespace(space, idx, fst);
      let snd = namespace(space, idx, snd);
      let val = Box::new(copy(space, idx, val));
      let nxt = Box::new(copy(space, idx, nxt));
      Dup{tag, fst, snd, val, nxt}
    },
    Fix{nam, bod} => {
      let nam = namespace(space, idx, nam);
      let bod = Box::new(copy(space, idx, bod));
      Fix{nam, bod}
    },
    Ann{val, typ} => {
      let val = Box::new(copy(space, idx, val));
      let typ = Box::new(copy(space, idx, typ));
      Ann{val, typ}
    },
    Var{nam} => {
      let nam = namespace(space, idx, nam);
      Var{nam}
    },
    Set => Set
  }
}

// Display macro.
impl std::fmt::Display for Term {
  fn fmt(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
    write!(f, "{}", String::from_utf8_lossy(&to_string(&self)))
  }
}

// Reduces an Interaction Calculus term through Interaction Combinators.
pub fn normalize(term: &Term, definition_book: &DefinitionBook) -> Term {
  let mut net : INet = new_inet();
  alloc_at(&mut net, &term, ROOT, &definition_book.definition_name_to_id);
  normal(&mut net, ROOT, definition_book);
  read_at(&net, ROOT, definition_book)
}

pub fn normalize_with_stats(term: &Term, definition_book: &DefinitionBook) -> (Term, u32) {
  let mut net = new_inet();
  alloc_at(&mut net, &term, ROOT, &definition_book.definition_name_to_id);
  normal(&mut net, ROOT, definition_book);
  let term = read_at(&net, ROOT, definition_book);
  (term, net.rules)
}
