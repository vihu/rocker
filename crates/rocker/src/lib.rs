use rustler::{Env, Term};

mod atoms;
mod db;

rustler::init!(
    "rocker",
    [
        db::lxcode,
        db::open,
        db::open_default,
        db::open_cf_default,
        db::destroy,
        db::repair,
        db::path,
        db::put,
        db::get,
        db::delete,
        db::tx,
        db::iterator,
        db::prefix_iterator,
        db::iterator_valid,
        db::next,
        db::create_cf_default,
        db::create_cf,
        db::list_cf,
        db::drop_cf,
        db::put_cf,
        db::get_cf,
        db::delete_cf,
        db::iterator_cf,
        db::prefix_iterator_cf
    ],
    load=load
    );

fn load(env: Env, _load_info: Term) -> bool {
    db::load(env);
}
