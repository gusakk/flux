//! Semantic analysis.

pub mod convert;

mod import;

mod infer;

#[macro_use]
pub mod types;
pub mod bootstrap;
pub mod check;
pub mod env;
pub mod fresh;
pub mod nodes;
pub mod sub;
pub mod walk;

#[cfg(test)]
mod tests;

#[allow(unused, non_snake_case)]
pub mod flatbuffers;

use crate::ast;
use crate::parser::parse_string;
use crate::semantic::convert::convert_with;
use crate::semantic::env::Environment;
use crate::semantic::fresh::Fresher;
use crate::semantic::import::Importer;
use derive_more::Display;

/// An error in semantic analysis.
#[derive(Debug, Display)]
#[display(fmt = "{}", msg)]
pub struct Error {
    msg: String,
}

impl From<nodes::Error> for Error {
    fn from(err: nodes::Error) -> Error {
        Error {
            msg: err.to_string(),
        }
    }
}

impl From<String> for Error {
    fn from(msg: String) -> Error {
        Error { msg }
    }
}

impl Importer for Option<()> {}

/// Get a semantic package from the given source and fresher
/// The returned semantic package is not type-inferred.
fn get_sem_pkg_from_source(source: &str, fresher: &mut Fresher) -> Result<nodes::Package, Error> {
    let file = parse_string("", source);
    let errs = ast::check::check(ast::walk::Node::File(&file));
    if !errs.is_empty() {
        return Err(Error {
            msg: format!("got errors on parsing: {:?}", errs),
        });
    }
    let ast_pkg: ast::Package = file.into();
    convert_with(ast_pkg, fresher).map_err(|err| err.into())
}

/// Get a type-inferred semantic package from the given Flux source.
pub fn convert_source(source: &str) -> Result<nodes::Package, Error> {
    let mut f = Fresher::default();
    let mut sem_pkg = get_sem_pkg_from_source(source, &mut f)?;
    // TODO(affo): add a stdlib Importer.
    let (_, sub) = nodes::infer_pkg_types(&mut sem_pkg, Environment::empty(false), &mut f, &None)?;
    Ok(nodes::inject_pkg_types(sem_pkg, &sub))
}
