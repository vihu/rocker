use rocksdb::{DB, DBCompactionStyle, Direction, IteratorMode, Options, WriteBatch};
use rocksdb::DBIterator;
use rustler::{Env, Error, Term};
use rustler::resource::ResourceArc;
use rustler::types::binary::{Binary, OwnedBinary};
use rustler::types::list::ListIterator;
use rustler::types::map::MapIterator;
use rustler::types::Atom;
use std::sync::{RwLock, Mutex};
use crate::atoms::{ok, error, vn1, undefined};

struct DbResource {
    db: RwLock<DB>,
    path: String,
}


struct Ref(Mutex<DBIterator>);

impl Ref {
    fn new(iter: DBIterator) -> ResourceArc<Ref> {
        ResourceArc::new(Ref(Mutex::new(iter)))
    }
}

type DbResourceArc = ResourceArc<DbResource>;

pub fn load(env: Env, _load_info: Term) -> bool {
    rustler::resource!(DbResource, env);
    rustler::resource!(Ref, env);
}

#[rustler::nif]
fn lxcode() -> Atom {
    vn1()
}

#[rustler::nif]
fn open(env: Env, db_path: String, iter: MapIterator) -> (Atom, DbResourceArc) {
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
            (ok(), resource).encode(env)
        }
        Err(e) => (error(), e.to_string()).encode(env)
    }
}


#[rustler::nif]
fn open_default(env: Env, db_path: String) -> (Atom, ResourceArc<DbResource>) {
    match DB::open_default(db_path.clone()) {
        Ok(db) => {
            let resource = ResourceArc::new(DbResource {
                db: RwLock::new(
                    db
                ),
                path: db_path.clone(),
            });
            (ok(), resource).encode(env)
        }
        Err(e) => (error(), e.to_string()).encode(env)
    }
}


#[rustler::nif]
fn open_cf_default(env: Env, db_path: String, iter: ListIterator) -> (Atom, ResourceArc<DbResource>) {
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

    (ok(), resource).encode(env)
}


#[rustler::nif]
fn destroy(env: Env, db_path: String) -> Atom {
    match DB::destroy(&Options::default(), db_path) {
        Ok(_) => Ok(ok()),
        Err(e) => (error(), e.to_string()).encode(env)
    }
}


#[rustler::nif]
fn repair(env: Env, db_path: String) -> Atom {
    match DB::repair(&Options::default(), db_path) {
        Ok(_) => Ok(ok()),
        Err(e) => (error(), e.to_string()).encode(env)
    }
}


#[rustler::nif]
fn path(env: Env, resource: DbResourceArc) -> (Atom, String) {
    let db_path = resource.path.to_string();
    (ok(), db_path).encode(env)
}


#[rustler::nif]
fn put(env: Env, resource: DbResourceArc, key: Binary, value: Binary) -> Atom {
    let db = resource.db.write().unwrap();
    match db.put(&key, &value) {
        Ok(_) => Ok(ok()),
        Err(e) => (error(), e.to_string()).encode(env)
    }
}


#[rustler::nif]
fn get(env: Env, resource: DbResourceArc, key: Binary) -> OwnedBinary {
    let db = resource.db.read().unwrap();
    match db.get(&key) {
        Ok(Some(v)) => {
            let mut value = OwnedBinary::new(v[..].len()).unwrap();
            value.clone_from_slice(&v[..]);
            Ok(value)
        }
        Ok(None) => (error(), undefined()).encode(env),
        Err(e) => (error(), e.to_string()).encode(env)
    }
}


#[rustler::nif]
fn delete(env: Env, resource: DbResourceArc, key: Binary) -> Atom {
    let db = resource.db.write().unwrap();
    match db.delete(&key) {
        Ok(_) => Ok(ok()),
        Err(e) => (error(), e.to_string()).encode(env)
    }
}


#[rustler::nif]
fn tx(env: Env, resource: DbResourceArc, iter: ListIterator) -> (Atom, usize) {
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
            Ok(_) => (ok(), applied).encode(env),
            Err(e) => (error(), e.to_string()).encode(env)
        }
    } else {
        (ok(), 0).encode(env)
    }
}


#[rustler::nif]
fn iterator(env: Env,
    resource: DbResourceArc,
    mode_terms: Vec<Term>) -> (Atom, ResourceArc<Ref>) {
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

    let resource = ResourceArc::new(Ref {
        iter: RwLock::new(
            db_iter,
        ),
    });

    (ok(), resource).encode(env)
}


#[rustler::nif]
fn prefix_iterator(env: Env,
    resource: DbResourceArc,
    prefix: Binary) -> (Atom, ResourceArc<Ref>) {
    let db = resource.db.read().unwrap();
    let db_iterator = db.prefix_iterator(&prefix);

    let resource = ResourceArc::new(Ref {
        iter: RwLock::new(
            db_iterator,
        ),
    });

    (ok(), resource).encode(env)
}


#[rustler::nif]
fn iterator_valid(env: Env, resource: ResourceArc<Ref>) -> (Atom, bool) {
    let iter = resource.iter.read().unwrap();
    (ok(), iter.valid()).encode(env)
}


#[rustler::nif]
fn next(env: Env, resource: ResourceArc<Ref>) -> (Atom, Binary, Binary) {
    let mut iter = resource.iter.write().unwrap();
    match iter.next() {
        None => (error(), undefined()).encode(env),
        Some((k, v)) => {
            let mut key = OwnedBinary::new(k[..].len()).unwrap();
            key.clone_from_slice(&k[..]);

            let mut value = OwnedBinary::new(v[..].len()).unwrap();
            value.clone_from_slice(&v[..]);

            (ok(), key.release(env), value.release(env)).encode(env)
        }
    }
}


#[rustler::nif]
fn create_cf_default(env: Env, resource: DbResourceArc, name: String) -> Atom {
    let mut db = resource.db.write().unwrap();
    let opts = Options::default();

    match db.create_cf(name.as_str(), &opts) {
        Ok(_) => Ok(ok()),
        Err(e) => (error(), e.to_string()).encode(env)
    }
}


#[rustler::nif]
fn create_cf(env: Env, resource: DbResourceArc, name: String, iter: MapIterator) -> Atom {
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
        Ok(_) => Ok(ok()),
        Err(e) => (error(), e.to_string()).encode(env)
    }
}


#[rustler::nif]
fn list_cf(env: Env, db_path: String) -> (Atom, Vec<String>) {
    match DB::list_cf(&Options::default(), db_path) {
        Ok(cfs) => (ok(), cfs).encode(env),
        Err(e) => (error(), e.to_string()).encode(env)
    }
}


#[rustler::nif]
fn drop_cf(env: Env, resource: DbResourceArc, name: String) -> Atom {
    let mut db = resource.db.write().unwrap();

    match db.drop_cf(name.as_str()) {
        Ok(_) => Ok(ok()),
        Err(e) => (error(), e.to_string()).encode(env)
    }
}


#[rustler::nif]
fn put_cf(env: Env, resource: DbResourceArc, cf: String, key: Binary, value: Binary) -> Atom {
    let db = resource.db.write().unwrap();
    let cf_handler = db.cf_handle(&cf.as_str()).unwrap();
    match db.put_cf(cf_handler, &key, &value) {
        Ok(_) => Ok(ok()),
        Err(e) => (error(), e.to_string()).encode(env)
    }
}

#[rustler::nif]
fn get_cf(env: Env, resource: DbResourceArc, cf: String, key: Binary) -> OwnedBinary {
    let db = resource.db.read().unwrap();
    let cf_handler = db.cf_handle(&cf.as_str()).unwrap();
    match db.get_cf(cf_handler, &key) {
        Ok(Some(v)) => {
            let mut value = OwnedBinary::new(v[..].len()).unwrap();
            value.clone_from_slice(&v[..]);
            Ok(value)
        }
        Ok(None) => Err(Error::Atom("not_found")),
        Err(e) => (error(), e.to_string()).encode(env)
    }
}

#[rustler::nif]
fn delete_cf(env: Env, resource: DbResourceArc, cf: String, key: Binary) -> Atom {
    let db = resource.db.read().unwrap();
    let cf_handler = db.cf_handle(&cf.as_str()).unwrap();
    match db.delete_cf(cf_handler, &key) {
        Ok(_) => Ok(ok()),
        Err(e) => (error(), e.to_string()).encode(env)
    }
}

#[rustler::nif]
fn iterator_cf(env: Env,
    resource: DbResourceArc,
    cf: String,
    mode_terms: Vec<Term>) -> (Atom, ResourceArc<Ref>) {
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

    let resource = ResourceArc::new(Ref {
        iter: RwLock::new(
            db_iter
        ),
    });

    (ok(), resource).encode(env)
}

#[rustler::nif]
fn prefix_iterator_cf(env: Env,
    resource: DbResourceArc,
    cf: String,
    prefix: Binary) -> (Atom, ResourceArc<Ref>) {
    let db = resource.db.read().unwrap();
    let cf_handler = db.cf_handle(&cf.as_str()).unwrap();
    let db_iter = db.prefix_iterator_cf(cf_handler, &prefix);

    let resource = ResourceArc::new(Ref {
        iter: RwLock::new(
            db_iter
        ),
    });

    (ok(), resource).encode(env)
}
