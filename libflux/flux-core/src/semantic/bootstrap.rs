//! Flux start-up.

use std::collections::HashSet;
use std::fs;
use std::io;
use std::path::{Path, PathBuf};

use crate::ast;
use crate::parser;
use crate::semantic::convert::convert_file;
use crate::semantic::env::Environment;
use crate::semantic::fresh::Fresher;
use crate::semantic::import::Importer;
use crate::semantic::infer;
use crate::semantic::infer::Constraints;
use crate::semantic::nodes;
use crate::semantic::nodes::infer_file;
use crate::semantic::sub::Substitutable;
use crate::semantic::types;
use crate::semantic::types::{
    MonoType, PolyType, PolyTypeMap, Property, Record, SemanticMap, TvarKinds,
};

use walkdir::WalkDir;

const PRELUDE: [&str; 2] = ["universe", "influxdata/influxdb"];

type AstFileMap = SemanticMap<String, ast::File>;

/// Error returned during bootstrap.
#[derive(Debug, PartialEq)]
pub struct Error {
    /// Error message.
    pub msg: String,
}

/// DocPackage represents the documentation for a package and its sub packages
#[derive(Debug, Serialize, Deserialize)]
pub struct DocPackage {
    /// the path to the documentation package
    pub path: String,
    /// the name of the comments package?
    pub name: String,
    /// the doc
    pub doc: String,
    /// the values of the comments
    pub values: Vec<DocValue>,
}

/// DocValue represents the documentation for a single value within a package.
/// Values include options, builtins or any variable assignment within the top level scope of a
/// package.
#[derive(Debug, Serialize, Deserialize)]
pub struct DocValue {
    /// the path to the package
    pub pkgpath: String,
    /// the name of the package
    pub name: String,
    /// the doc
    pub doc: String,
    /// the type of the comments
    pub typ: String,
}

impl From<io::Error> for Error {
    fn from(err: io::Error) -> Error {
        Error {
            msg: format!("{:?}", err),
        }
    }
}

impl From<nodes::Error> for Error {
    fn from(err: nodes::Error) -> Error {
        Error {
            msg: err.to_string(),
        }
    }
}

impl From<types::Error> for Error {
    fn from(err: types::Error) -> Error {
        Error {
            msg: err.to_string(),
        }
    }
}

impl From<infer::Error> for Error {
    fn from(err: infer::Error) -> Error {
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

impl From<&str> for Error {
    fn from(msg: &str) -> Error {
        Error {
            msg: msg.to_string(),
        }
    }
}
/// Infers the types of the standard library returning two [`PolyTypeMap`]s, one for the prelude
/// and one for the standard library, as well as a type variable [`Fresher`].
#[allow(clippy::type_complexity)]
pub fn infer_stdlib() -> Result<(PolyTypeMap, PolyTypeMap, Fresher, Vec<String>, AstFileMap), Error>
{
    let mut f = Fresher::default();

    let dir = "../../stdlib";
    let files = file_map(parse_flux_files(dir)?);
    let rerun_if_changed = compute_file_dependencies(dir);

    let (prelude, importer) = infer_pre(&mut f, &files)?;
    let importer = infer_std(&mut f, &files, prelude.clone(), importer)?;

    Ok((prelude, importer, f, rerun_if_changed, files))
}

/// new stdlib docs function
pub fn stdlib_docs(
    lib: &PolyTypeMap,
    files: &AstFileMap,
) -> Result<Vec<DocPackage>, Box<dyn std::error::Error>> {
    //let pkg = docs::walk_pkg(&args.pkg, &args.pkg)?;
    let mut docs = Vec::new();
    for (path, file) in files {
        let pkg = generate_docs(path.clone(), &lib, file)?;
        docs.push(pkg);
    }
    Ok(docs)
}

// Generates the docs by parsing the sources and checking type inference.
fn generate_docs(
    path: String,
    types: &PolyTypeMap,
    file: &ast::File,
) -> Result<DocPackage, Box<dyn std::error::Error>> {
    // construct the package documentation
    // use type inference to determine types of all values
    //let sem_pkg = analyze(pkg.clone())?;
    //let types = pkg_types(&sem_pkg);

    let mut doc = String::new();
    let values = generate_values(&file, &types, &path)?;
    if let Some(comment) = &file.package {
        doc = comments_to_string(&comment.base.comments);
    }
    //TODO check if package name exists and if it doesn't throw an error message
    Ok(DocPackage {
        path,
        name: file.package.clone().unwrap().name.name,
        doc,
        values,
    })
}

// Generates docs for the values in a given source file.
fn generate_values(
    f: &ast::File,
    types: &PolyTypeMap,
    pkgpath: &str,
) -> Result<Vec<DocValue>, Box<dyn std::error::Error>> {
    let mut values: Vec<DocValue> = Vec::new();
    //println!("{:?}", types);
    for stmt in &f.body {
        match stmt {
            ast::Statement::Variable(s) => {
                let doc = comments_to_string(&s.id.base.comments);
                let name = s.id.name.clone();
                //println!("{}", name);
                if !types.contains_key(&name) {
                    continue;
                }
                let typ = format!("{}", types[&name].normal());
                values.push(DocValue {
                    pkgpath: pkgpath.to_string(),
                    name: name.clone(),
                    doc,
                    typ,
                });
            }
            ast::Statement::Builtin(s) => {
                let doc = comments_to_string(&s.base.comments);
                let name = s.id.name.clone();
                if !types.contains_key(&name) {
                    continue;
                }
                let typ = format!("{}", types[&name].normal());
                values.push(DocValue {
                    pkgpath: pkgpath.to_string(),
                    name: name.clone(),
                    doc,
                    typ,
                });
            }
            ast::Statement::Option(s) => {
                if let ast::Assignment::Variable(v) = &s.assignment {
                    let doc = comments_to_string(&s.base.comments);
                    let name = v.id.name.clone();
                    if !types.contains_key(&name) {
                        continue;
                    }
                    let typ = format!("{}", types[&name].normal());
                    values.push(DocValue {
                        pkgpath: pkgpath.to_string(),
                        name: name.clone(),
                        doc,
                        typ,
                    });
                }
            }
            _ => {}
        }
    }
    Ok(values)
}

fn comments_to_string(comments: &[ast::Comment]) -> String {
    let mut s = String::new();
    if !comments.is_empty() {
        for c in comments {
            s.push_str(c.text.as_str().strip_prefix("//").unwrap());
        }
    }
    comrak::markdown_to_html(s.as_str(), &comrak::ComrakOptions::default())
}

fn compute_file_dependencies(root: &str) -> Vec<String> {
    // Iterate through each ast file and canonicalize the
    // file path to an absolute path.
    // Canonicalize the root path to the absolute directory.
    let rootpath = std::env::current_dir()
        .unwrap()
        .join(root)
        .canonicalize()
        .unwrap();
    WalkDir::new(rootpath)
        .into_iter()
        .filter_map(|r| r.ok())
        .filter(|r| r.path().is_dir() || (r.path().is_file() && r.path().ends_with(".flux")))
        .map(|r| path_to_string(r.path()))
        .collect()
}

fn path_to_string(path: &Path) -> String {
    path.to_str().expect("valid path").to_string()
}

#[allow(clippy::type_complexity)]
fn infer_pre(f: &mut Fresher, files: &AstFileMap) -> Result<(PolyTypeMap, PolyTypeMap), Error> {
    let mut prelude = PolyTypeMap::new();
    let mut imports = PolyTypeMap::new();
    for name in &PRELUDE {
        let (types, importer) = infer_pkg(name, f, files, PolyTypeMap::new(), imports)?;
        for (k, v) in types {
            prelude.insert(k, v);
        }
        imports = importer;
    }
    Ok((prelude, imports))
}

#[allow(clippy::type_complexity)]
fn infer_std(
    f: &mut Fresher,
    files: &AstFileMap,
    prelude: PolyTypeMap,
    mut imports: PolyTypeMap,
) -> Result<PolyTypeMap, Error> {
    for (path, _) in files.iter() {
        if imports.contains_key(path) {
            continue;
        }
        let (types, mut importer) = infer_pkg(path, f, files, prelude.clone(), imports)?;
        importer.insert(path.to_string(), build_polytype(types, f)?);
        imports = importer;
    }
    Ok(imports)
}

// Recursively parse all flux files within a directory.
fn parse_flux_files(path: &str) -> io::Result<Vec<ast::File>> {
    let mut files = Vec::new();
    let entries = WalkDir::new(PathBuf::from(path))
        .into_iter()
        .filter_map(|r| r.ok())
        .filter(|r| r.path().is_file());

    for entry in entries {
        if let Some(path) = entry.path().to_str() {
            if path.ends_with(".flux") && !path.ends_with("_test.flux") {
                files.push(parser::parse_string(
                    path.rsplitn(2, "/stdlib/").collect::<Vec<&str>>()[0],
                    &fs::read_to_string(path)?,
                ));
            }
        }
    }
    Ok(files)
}

// Associates an import path with each file
fn file_map(files: Vec<ast::File>) -> AstFileMap {
    files.into_iter().fold(AstFileMap::new(), |mut acc, file| {
        let name = file.name.rsplitn(2, '/').collect::<Vec<&str>>()[1].to_string();
        acc.insert(name, file);
        acc
    })
}

fn imports(file: &ast::File) -> Vec<&str> {
    let mut dependencies = Vec::new();
    for import in &file.imports {
        dependencies.push(&import.path.value[..]);
    }
    dependencies
}

// Determines the dependencies of a package. That is, all packages
// that must be evaluated before the package in question. Each
// dependency is added to the `deps` vector in evaluation order.
#[allow(clippy::type_complexity)]
fn dependencies<'a>(
    name: &'a str,
    pkgs: &'a AstFileMap,
    mut deps: Vec<&'a str>,
    mut seen: HashSet<&'a str>,
    mut done: HashSet<&'a str>,
) -> Result<(Vec<&'a str>, HashSet<&'a str>, HashSet<&'a str>), Error> {
    if seen.contains(name) && !done.contains(name) {
        Err(Error {
            msg: format!(r#"package "{}" depends on itself"#, name),
        })
    } else {
        seen.insert(name);
        match pkgs.get(name) {
            None => Err(Error {
                msg: format!(r#"package "{}" not found"#, name),
            }),
            Some(file) => {
                for name in imports(file) {
                    let (x, y, z) = dependencies(name, pkgs, deps, seen, done)?;
                    deps = x;
                    seen = y;
                    done = z;
                    if !deps.contains(&name) {
                        deps.push(name);
                    }
                }
                done.insert(name);
                Ok((deps, seen, done))
            }
        }
    }
}

/// Constructs a polytype, or more specifically a generic record type, from a hash map.
pub fn build_polytype(from: PolyTypeMap, f: &mut Fresher) -> Result<PolyType, Error> {
    let (r, cons) = build_record(from, f);
    let mut kinds = TvarKinds::new();
    let sub = infer::solve(&cons, &mut kinds, f)?;
    Ok(infer::generalize(
        &Environment::empty(false),
        &kinds,
        MonoType::Record(Box::new(r)).apply(&sub),
    ))
}

fn build_record(from: PolyTypeMap, f: &mut Fresher) -> (Record, Constraints) {
    let mut r = Record::Empty;
    let mut cons = Constraints::empty();

    for (name, poly) in from {
        let (ty, constraints) = infer::instantiate(
            poly.clone(),
            f,
            ast::SourceLocation {
                file: None,
                start: ast::Position::default(),
                end: ast::Position::default(),
                source: None,
            },
        );
        r = Record::Extension {
            head: Property { k: name, v: ty },
            tail: MonoType::Record(Box::new(r)),
        };
        cons = cons + constraints;
    }
    (r, cons)
}

// Infer the types in a package(file), returning a hash map containing
// the inferred types along with a possibly updated map of package imports.
//
#[allow(clippy::type_complexity)]
fn infer_pkg(
    name: &str,           // name of package to infer
    f: &mut Fresher,      // type variable fresher
    files: &AstFileMap,   // files available for inference
    prelude: PolyTypeMap, // prelude types
    imports: PolyTypeMap, // types available for import
) -> Result<
    (
        PolyTypeMap, // inferred types
        PolyTypeMap, // types available for import (possibly updated)
    ),
    Error,
> {
    // Determine the order in which we must infer dependencies
    let (deps, _, _) = dependencies(name, files, Vec::new(), HashSet::new(), HashSet::new())?;

    let mut imports = imports;

    // Infer all dependencies
    for pkg in deps {
        if imports.import(pkg).is_none() {
            let file = files.get(pkg);
            if file.is_none() {
                return Err(Error {
                    msg: format!(r#"package "{}" not found"#, pkg),
                });
            }
            let file = file.unwrap().to_owned();

            let env = infer_file(
                &mut convert_file(file, f)?,
                Environment::new(prelude.clone().into()),
                f,
                &imports,
            )?
            .0;

            imports.insert(pkg.to_string(), build_polytype(env.values, f)?);
        }
    }

    let file = files.get(name);
    if file.is_none() {
        return Err(Error {
            msg: format!("package '{}' not found", name),
        });
    }
    let file = file.unwrap().to_owned();

    let env = infer_file(
        &mut convert_file(file, f)?,
        Environment::new(prelude.into()),
        f,
        &imports,
    )?
    .0;

    Ok((env.values, imports))
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::ast::get_err_type_expression;
    use crate::parser;
    use crate::parser::parse_string;
    use crate::semantic::convert::convert_polytype;

    #[test]
    fn infer_program() -> Result<(), Error> {
        let a = r#"
            f = (x) => x
        "#;
        let b = r#"
            import "a"

            builtin x : int

            y = a.f(x: x)
        "#;
        let c = r#"
            import "b"

            z = b.y
        "#;
        let files = semantic_map! {
            String::from("a") => parse_string("a.flux", a),
            String::from("b") => parse_string("b.flux", b),
            String::from("c") => parse_string("c.flux", c),
        };
        let (types, imports) = infer_pkg(
            "c",
            &mut Fresher::from(1),
            &files,
            PolyTypeMap::new(),
            PolyTypeMap::new(),
        )?;

        let want = semantic_map! {
            String::from("z") => {
                    let mut p = parser::Parser::new("int");
                    let typ_expr = p.parse_type_expression();
                    let err = get_err_type_expression(typ_expr.clone());
                    if err != "" {
                        panic!(
                            "TypeExpression parsing failed for int. {:?}", err
                        );
                    }
                    convert_polytype(typ_expr, &mut Fresher::default())?
            },
        };
        if want != types {
            return Err(Error {
                msg: format!(
                    "unexpected inference result:\n\nwant: {:?}\n\ngot: {:?}",
                    want, types
                ),
            });
        }

        let want = semantic_map! {
            String::from("a") => {
            let mut p = parser::Parser::new("{f: (x: A) => A}");
                    let typ_expr = p.parse_type_expression();
                    let err = get_err_type_expression(typ_expr.clone());
                    if err != "" {
                        panic!(
                            "TypeExpression parsing failed for int. {:?}", err
                        );
                    }
                    convert_polytype(typ_expr, &mut Fresher::default())?
            },
            String::from("b") => {
            let mut p = parser::Parser::new("{x: int , y: int}");
                    let typ_expr = p.parse_type_expression();
                    let err = get_err_type_expression(typ_expr.clone());
                    if err != "" {
                        panic!(
                            "TypeExpression parsing failed for int. {:?}", err
                        );
                    }
                    convert_polytype(typ_expr, &mut Fresher::default())?
            },
        };
        if want != imports {
            return Err(Error {
                msg: format!(
                    "unexpected type importer:\n\nwant: {:?}\n\ngot: {:?}",
                    want, types
                ),
            });
        }

        Ok(())
    }

    #[test]
    fn prelude_dependencies() {
        let files = file_map(parse_flux_files("../../stdlib").unwrap());

        let r = PRELUDE.iter().try_fold(
            (Vec::new(), HashSet::new(), HashSet::new()),
            |(deps, seen, done), name| dependencies(name, &files, deps, seen, done),
        );

        let names = r.unwrap().0;

        assert_eq!(vec!["system", "date", "math", "strings", "regexp"], names,);
    }

    #[test]
    fn cyclic_dependency() {
        let a = r#"
            import "b"
        "#;
        let b = r#"
            import "a"
        "#;
        let files = semantic_map! {
            String::from("a") => parse_string("a.flux", a),
            String::from("b") => parse_string("b.flux", b),
        };
        let got_err = dependencies("b", &files, Vec::new(), HashSet::new(), HashSet::new())
            .expect_err("expected cyclic dependency error");

        assert_eq!(
            Error {
                msg: r#"package "b" depends on itself"#.to_string()
            },
            got_err
        );
    }
}
