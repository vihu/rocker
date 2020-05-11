extern crate lazy_static;
extern crate rocksdb;

#[macro_use]
extern crate rustler;

use rocksdb::{DB, DBCompactionStyle, Direction, IteratorMode, Options, WriteBatch};
use rocksdb::DBIterator;
use rustler::{Env, NifResult, Error, Term};
use rustler::resource::ResourceArc;
use rustler::types::binary::{Binary, OwnedBinary};
use rustler::types::list::ListIterator;
use rustler::types::map::MapIterator;
use rustler::types::Atom;
use std::sync::RwLock;

mod atoms {
    rustler::atoms! {
        ok,
        err,
        vn1,
        notfound
    }
}

struct DbResource {
    db: RwLock<DB>,
    path: String,
}

struct IteratorResource {
    iter: RwLock<DBIterator>
}

type DbResourceArc = ResourceArc<DbResource>;
type IteratorResourceArc = ResourceArc<IteratorResource>;

init!(
    "rocker",
    [
        lxcode,
        open,
        open_default,
        open_cf_default,
        destroy,
        repair,
        path,
        put,
        get,
        delete,
        tx,
        iterator,
        prefix_iterator,
        iterator_valid,
        next,
        create_cf_default,
        create_cf,
        list_cf,
        drop_cf,
        put_cf,
        get_cf,
        delete_cf,
        iterator_cf,
        prefix_iterator_cf
    ],
    load=on_load
    );

fn on_load<'a>(env: Env<'a>, _load_info: Term<'a>) -> bool {
    resource!(DbResource, env);
    resource!(IteratorResource, env);
    true
}

#[rustler::nif]
fn lxcode() -> Atom {
    atoms::vn1()
}

#[rustler::nif]
fn open<'a>(db_path: String, iter: MapIterator) -> NifResult<(Atom, DbResourceArc)> {
    let mut opts = Options::default();
    for (key, value) in iter {
        let param = key.atom_to_string()?;
        match param.as_str() {
            "create_if_missing" => {
                if value.atom_to_string()?.as_str() == "true" {
                    opts.create_if_missing(true);
                }
            }
            "create_missing_column_families" => {
                if value.atom_to_string()?.as_str() == "true" {
                    opts.create_missing_column_families(true);
                }
            }
            "set_max_open_files" => {
                let limit: i32 = value.decode()?;
                opts.set_max_open_files(limit);
            }
            "set_use_fsync" => {
                if value.atom_to_string()?.as_str() == "true" {
                    opts.set_use_fsync(true);
                }
            }
            "set_bytes_per_sync" => {
                let limit: u64 = value.decode()?;
                opts.set_bytes_per_sync(limit);
            }
            "optimize_for_point_lookup" => {
                let limit: u64 = value.decode()?;
                opts.optimize_for_point_lookup(limit);
            }
            "set_table_cache_num_shard_bits" => {
                let limit: i32 = value.decode()?;
                opts.set_table_cache_num_shard_bits(limit);
            }
            "set_max_write_buffer_number" => {
                let limit: i32 = value.decode()?;
                opts.set_max_write_buffer_number(limit);
            }
            "set_write_buffer_size" => {
                let limit: usize = value.decode()?;
                opts.set_write_buffer_size(limit);
            }
            "set_target_file_size_base" => {
                let limit: u64 = value.decode()?;
                opts.set_target_file_size_base(limit);
            }
            "set_min_write_buffer_number_to_merge" => {
                let limit: i32 = value.decode()?;
                opts.set_min_write_buffer_number_to_merge(limit);
            }
            "set_level_zero_stop_writes_trigger" => {
                let limit: i32 = value.decode()?;
                opts.set_level_zero_stop_writes_trigger(limit);
            }
            "set_level_zero_slowdown_writes_trigger" => {
                let limit: i32 = value.decode()?;
                opts.set_level_zero_slowdown_writes_trigger(limit);
            }
            "set_max_background_compactions" => {
                let limit: i32 = value.decode()?;
                opts.set_max_background_compactions(limit);
            }
            "set_max_background_flushes" => {
                let limit: i32 = value.decode()?;
                opts.set_max_background_flushes(limit);
            }
            "set_disable_auto_compactions" => {
                if value.atom_to_string()?.as_str() == "true" {
                    opts.set_disable_auto_compactions(true);
                }
            }
            "set_compaction_style" => {
                let style = value.atom_to_string()?;
                if style == "level" {
                    opts.set_compaction_style(DBCompactionStyle::Level);
                } else if style == "universal" {
                    opts.set_compaction_style(DBCompactionStyle::Universal);
                } else if style == "fifo" {
                    opts.set_compaction_style(DBCompactionStyle::Fifo);
                }
            }
            "prefix_length" => {
                let limit: usize = value.decode()?;
                let prefix_extractor = rocksdb::SliceTransform::create_fixed_prefix(limit);
                opts.set_prefix_extractor(prefix_extractor);
            }
            _ => {}
        }
    }

    match DB::open(&opts, db_path.clone()) {
        Ok(db) => {
            let resource = ResourceArc::new(DbResource {
                db: RwLock::new(
                    db
                ),
                path: db_path.clone(),
            });
            Ok((atoms::ok(), resource))
        }
        Err(_e) => Err(Error::Atom("unable_to_open"))
    }
}


#[rustler::nif]
fn open_default<'a>(db_path: String) -> NifResult<(Atom, ResourceArc<DbResource>)> {
    match DB::open_default(db_path.clone()) {
        Ok(db) => {
            let resource = ResourceArc::new(DbResource {
                db: RwLock::new(
                    db
                ),
                path: db_path.clone(),
            });
            Ok((atoms::ok(), resource))
        }
        Err(_e) => Err(Error::Atom("unable_to_open_default"))
    }
}


#[rustler::nif]
fn open_cf_default<'a>(db_path: String, iter: ListIterator) -> NifResult<(Atom, ResourceArc<DbResource>)> {
    let mut cfs: Vec<String> = Vec::new();
    for elem in iter {
        let name: String = elem.decode()?;
        cfs.push(name);
    }
    let cfs2: Vec<&str> = cfs.iter().map(|s| &**s).collect();
    let resource = ResourceArc::new(DbResource {
        db: RwLock::new(
            DB::open_cf(&Options::default(), db_path.clone(), &cfs2).unwrap()
        ),
        path: db_path.clone(),
    });

    Ok((atoms::ok(), resource))
}


#[rustler::nif]
fn destroy<'a>(db_path: String) -> NifResult<Atom> {
    match DB::destroy(&Options::default(), db_path) {
        Ok(_) => Ok(atoms::ok()),
        Err(_e) => Err(Error::Atom("unable_to_destroy"))
    }
}


#[rustler::nif]
fn repair<'a>(db_path: String) -> NifResult<Atom> {
    match DB::repair(&Options::default(), db_path) {
        Ok(_) => Ok(atoms::ok()),
        Err(_e) => Err(Error::Atom("unable_to_repair"))
    }
}


#[rustler::nif]
fn path<'a>(resource: DbResourceArc) -> NifResult<(Atom, String)> {
    let db_path = resource.path.to_string();
    Ok((atoms::ok(), db_path))
}


#[rustler::nif]
fn put<'a>(resource: DbResourceArc, key: Binary, value: Binary) -> NifResult<Atom> {
    let db = resource.db.write().unwrap();
    match db.put(&key, &value) {
        Ok(_) => Ok(atoms::ok()),
        Err(_e) => Err(Error::Atom("unable_to_put"))
    }
}


#[rustler::nif]
fn get<'a>(resource: DbResourceArc, key: Binary) -> NifResult<OwnedBinary> {
    let db = resource.db.read().unwrap();
    match db.get(&key) {
        Ok(Some(v)) => {
            let mut value = OwnedBinary::new(v[..].len()).unwrap();
            value.clone_from_slice(&v[..]);
            Ok(value)
        }
        Ok(None) => Err(Error::Atom("not_found")),
        Err(_e) => Err(Error::Atom("unable_to_get"))
    }
}


#[rustler::nif]
fn delete<'a>(resource: DbResourceArc, key: Binary) -> NifResult<Atom> {
    let db = resource.db.write().unwrap();
    match db.delete(&key) {
        Ok(_) => Ok(atoms::ok()),
        Err(_e) => Err(Error::Atom("unable_to_delete"))
    }
}


#[rustler::nif]
fn tx<'a>(resource: DbResourceArc, iter: ListIterator) -> NifResult<(Atom, usize)> {
    let db = resource.db.write().unwrap();
    let mut batch = WriteBatch::default();
    for elem in iter {
        let terms: Vec<Term> = ::rustler::types::tuple::get_tuple(elem)?;
        if terms.len() >= 2 {
            let op: String = terms[0].atom_to_string()?;
            match op.as_str() {
                "put" => {
                    let key: Binary = terms[1].decode()?;
                    let val: Binary = terms[2].decode()?;
                    let _ = batch.put(&key, &val);
                }
                "put_cf" => {
                    let cf: String = terms[1].decode()?;
                    let key: Binary = terms[2].decode()?;
                    let value: Binary = terms[3].decode()?;
                    let cf_handler = db.cf_handle(&cf.as_str()).unwrap();
                    let _ = batch.put_cf(cf_handler, &key, &value);
                }
                "delete" => {
                    let key: Binary = terms[1].decode()?;
                    let _ = batch.delete(&key);
                }
                "delete_cf" => {
                    let cf: String = terms[1].decode()?;
                    let key: Binary = terms[2].decode()?;
                    let cf_handler = db.cf_handle(&cf.as_str()).unwrap();
                    let _ = batch.delete_cf(cf_handler, &key);
                }
                _ => {}
            }
        }
    }
    if batch.len() > 0 {
        let applied = batch.len();
        match db.write(batch) {
            Ok(_) => Ok((atoms::ok(), applied)),
            Err(_e) => Err(Error::Atom("unable_to_apply_tx"))
        }
    } else {
        Ok((atoms::ok(), 0))
    }
}


#[rustler::nif]
fn iterator<'a>(
    resource: DbResourceArc,
    mode_terms: Vec<Term>) -> NifResult<(Atom, ResourceArc<IteratorResource>)> {
    let db = resource.db.read().unwrap();
    let mut db_iter = db.iterator(IteratorMode::Start);
    if mode_terms.len() >= 1 {
        let mode: String = mode_terms[0].atom_to_string()?;
        match mode.as_str() {
            "end" => db_iter = db.iterator(IteratorMode::End),
            "from" => {
                let from: Binary = mode_terms[1].decode()?;
                if mode_terms.len() == 3 {
                    let direction: String = mode_terms[2].atom_to_string()?;
                    db_iter = match direction.as_str() {
                        "reverse" => db.iterator(IteratorMode::From(&from, Direction::Reverse)),
                        _ => db.iterator(IteratorMode::From(&from, Direction::Forward)),
                    }
                } else {
                    db_iter = db.iterator(IteratorMode::From(&from, Direction::Forward));
                }
            }
            _ => {}
        }
    }

    let resource = ResourceArc::new(IteratorResource {
        iter: RwLock::new(
            db_iter,
        ),
    });

    Ok((atoms::ok(), resource))
}


#[rustler::nif]
fn prefix_iterator<'a>(
    resource: DbResourceArc,
    prefix: Binary) -> NifResult<(Atom, ResourceArc<IteratorResource>)> {
    let db = resource.db.read().unwrap();
    let db_iterator = db.prefix_iterator(&prefix);

    let resource = ResourceArc::new(IteratorResource {
        iter: RwLock::new(
            db_iterator,
        ),
    });

    Ok((atoms::ok(), resource))
}


#[rustler::nif]
fn iterator_valid<'a>(resource: IteratorResourceArc) -> NifResult<(Atom, bool)> {
    let iter = resource.iter.read().unwrap();
    Ok((atoms::ok(), iter.valid()))
}


#[rustler::nif]
fn next<'a>(env: Env<'a>, resource: IteratorResourceArc) -> NifResult<(Atom, Binary<'a>, Binary<'a>)> {
    let mut iter = resource.iter.write().unwrap();
    match iter.next() {
        None => Err(Error::Atom("not_found")),
        Some((k, v)) => {
            let mut key = OwnedBinary::new(k[..].len()).unwrap();
            key.clone_from_slice(&k[..]);

            let mut value = OwnedBinary::new(v[..].len()).unwrap();
            value.clone_from_slice(&v[..]);

            Ok((atoms::ok(), key.release(env), value.release(env)))
        }
    }
}


#[rustler::nif]
fn create_cf_default<'a>(resource: DbResourceArc, name: String) -> NifResult<Atom> {
    let mut db = resource.db.write().unwrap();
    let opts = Options::default();

    match db.create_cf(name.as_str(), &opts) {
        Ok(_) => Ok(atoms::ok()),
        Err(_e) => Err(Error::Atom("unable_to_create_cf_default"))
    }
}


#[rustler::nif]
fn create_cf<'a>(resource: DbResourceArc, name: String, iter: MapIterator) -> NifResult<Atom> {
    let mut db = resource.db.write().unwrap();

    let mut opts = Options::default();
    for (key, value) in iter {
        let param = key.atom_to_string()?;
        match param.as_str() {
            "create_if_missing" => {
                if value.atom_to_string()?.as_str() == "true" {
                    opts.create_if_missing(true);
                }
            }
            "create_missing_column_families" => {
                if value.atom_to_string()?.as_str() == "true" {
                    opts.create_missing_column_families(true);
                }
            }
            "set_max_open_files" => {
                let limit: i32 = value.decode()?;
                opts.set_max_open_files(limit);
            }
            "set_use_fsync" => {
                if value.atom_to_string()?.as_str() == "true" {
                    opts.set_use_fsync(true);
                }
            }
            "set_bytes_per_sync" => {
                let limit: u64 = value.decode()?;
                opts.set_bytes_per_sync(limit);
            }
            "optimize_for_point_lookup" => {
                let limit: u64 = value.decode()?;
                opts.optimize_for_point_lookup(limit);
            }
            "set_table_cache_num_shard_bits" => {
                let limit: i32 = value.decode()?;
                opts.set_table_cache_num_shard_bits(limit);
            }
            "set_max_write_buffer_number" => {
                let limit: i32 = value.decode()?;
                opts.set_max_write_buffer_number(limit);
            }
            "set_write_buffer_size" => {
                let limit: usize = value.decode()?;
                opts.set_write_buffer_size(limit);
            }
            "set_target_file_size_base" => {
                let limit: u64 = value.decode()?;
                opts.set_target_file_size_base(limit);
            }
            "set_min_write_buffer_number_to_merge" => {
                let limit: i32 = value.decode()?;
                opts.set_min_write_buffer_number_to_merge(limit);
            }
            "set_level_zero_stop_writes_trigger" => {
                let limit: i32 = value.decode()?;
                opts.set_level_zero_stop_writes_trigger(limit);
            }
            "set_level_zero_slowdown_writes_trigger" => {
                let limit: i32 = value.decode()?;
                opts.set_level_zero_slowdown_writes_trigger(limit);
            }
            "set_max_background_compactions" => {
                let limit: i32 = value.decode()?;
                opts.set_max_background_compactions(limit);
            }
            "set_max_background_flushes" => {
                let limit: i32 = value.decode()?;
                opts.set_max_background_flushes(limit);
            }
            "set_disable_auto_compactions" => {
                if value.atom_to_string()?.as_str() == "true" {
                    opts.set_disable_auto_compactions(true);
                }
            }
            "set_compaction_style" => {
                let style = value.atom_to_string()?;
                if style == "level" {
                    opts.set_compaction_style(DBCompactionStyle::Level);
                } else if style == "universal" {
                    opts.set_compaction_style(DBCompactionStyle::Universal);
                } else if style == "fifo" {
                    opts.set_compaction_style(DBCompactionStyle::Fifo);
                }
            }
            "prefix_length" => {
                let limit: usize = value.decode()?;
                let prefix_extractor = rocksdb::SliceTransform::create_fixed_prefix(limit);
                opts.set_prefix_extractor(prefix_extractor);
            }
            _ => {}
        }
    }

    match db.create_cf(name.as_str(), &opts) {
        Ok(_) => Ok(atoms::ok()),
        Err(_e) => Err(Error::Atom("unable_to_create_cf"))
    }
}


#[rustler::nif]
fn list_cf<'a>(db_path: String) -> NifResult<(Atom, Vec<String>)> {
    match DB::list_cf(&Options::default(), db_path) {
        Ok(cfs) => Ok((atoms::ok(), cfs)),
        Err(_e) => Err(Error::Atom("unable_to_list_cf"))
    }
}


#[rustler::nif]
fn drop_cf<'a>(resource: DbResourceArc, name: String) -> NifResult<Atom> {
    let mut db = resource.db.write().unwrap();

    match db.drop_cf(name.as_str()) {
        Ok(_) => Ok(atoms::ok()),
        Err(_e) => Err(Error::Atom("unable_to_drop_cf"))
    }
}


#[rustler::nif]
fn put_cf<'a>(resource: DbResourceArc, cf: String, key: Binary, value: Binary) -> NifResult<Atom> {
    let db = resource.db.write().unwrap();
    let cf_handler = db.cf_handle(&cf.as_str()).unwrap();
    match db.put_cf(cf_handler, &key, &value) {
        Ok(_) => Ok(atoms::ok()),
        Err(_e) => Err(Error::Atom("unable_to_put_cf"))
    }
}

#[rustler::nif]
fn get_cf<'a>(resource: DbResourceArc, cf: String, key: Binary) -> NifResult<OwnedBinary> {
    let db = resource.db.read().unwrap();
    let cf_handler = db.cf_handle(&cf.as_str()).unwrap();
    match db.get_cf(cf_handler, &key) {
        Ok(Some(v)) => {
            let mut value = OwnedBinary::new(v[..].len()).unwrap();
            value.clone_from_slice(&v[..]);
            Ok(value)
        }
        Ok(None) => Err(Error::Atom("not_found")),
        Err(_e) => Err(Error::Atom("unable_to_get_cf"))
    }
}

#[rustler::nif]
fn delete_cf<'a>(resource: DbResourceArc, cf: String, key: Binary) -> NifResult<Atom> {
    let db = resource.db.read().unwrap();
    let cf_handler = db.cf_handle(&cf.as_str()).unwrap();
    match db.delete_cf(cf_handler, &key) {
        Ok(_) => Ok(atoms::ok()),
        Err(_e) => Err(Error::Atom("unable_to_delete_cf"))
    }
}

#[rustler::nif]
fn iterator_cf<'a>(
    resource: DbResourceArc,
    cf: String,
    mode_terms: Vec<Term>) -> NifResult<(Atom, ResourceArc<IteratorResource>)> {
    let db = resource.db.read().unwrap();
    let cf_handler = db.cf_handle(&cf.as_str()).unwrap();
    let mut db_iter = db.iterator_cf(cf_handler, IteratorMode::Start);
    if mode_terms.len() >= 1 {
        let mode: String = mode_terms[0].atom_to_string()?;
        match mode.as_str() {
            "end" => db_iter = db.iterator_cf(cf_handler, IteratorMode::End),
            "from" => {
                let from: Binary = mode_terms[1].decode()?;
                if mode_terms.len() == 3 {
                    let direction: String = mode_terms[2].atom_to_string()?;
                    db_iter = match direction.as_str() {
                        "reverse" => db.iterator_cf(cf_handler, IteratorMode::From(&from, Direction::Reverse)),
                        _ => db.iterator_cf(cf_handler, IteratorMode::From(&from, Direction::Forward)),
                    }
                } else {
                    db_iter = db.iterator_cf(cf_handler, IteratorMode::From(&from, Direction::Forward));
                }
            }
            _ => {}
        }
    }

    let resource = ResourceArc::new(IteratorResource {
        iter: RwLock::new(
            db_iter
        ),
    });

    Ok((atoms::ok(), resource))
}

#[rustler::nif]
fn prefix_iterator_cf<'a>(
    resource: DbResourceArc,
    cf: String,
    prefix: Binary) -> NifResult<(Atom, ResourceArc<IteratorResource>)> {
    let db = resource.db.read().unwrap();
    let cf_handler = db.cf_handle(&cf.as_str()).unwrap();
    let db_iter = db.prefix_iterator_cf(cf_handler, &prefix);

    let resource = ResourceArc::new(IteratorResource {
        iter: RwLock::new(
            db_iter
        ),
    });

    Ok((atoms::ok(), resource))
}
