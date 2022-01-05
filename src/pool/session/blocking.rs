//! Session pool blocking mode implementation

use super::SessionPool;
use crate::{Result, oci::{self, *}, Environment, Connection};
use std::{ptr, marker::PhantomData};

impl<'a> SessionPool<'a> {
    pub(crate) fn new(env: &'a Environment, dbname: &str, username: &str, password: &str, min: usize, inc: usize, max: usize) -> Result<Self> {
        let err = Handle::<OCIError>::new(env)?;
        let info = Handle::<OCIAuthInfo>::new(env)?;
        info.set_attr(OCI_ATTR_DRIVER_NAME, "sibyl", &err)?;

        let pool = Handle::<OCISPool>::new(env)?;
        pool.set_attr(OCI_ATTR_SPOOL_AUTH, info.get_ptr(), &err)?;

        let mut pool_name_ptr = ptr::null::<u8>();
        let mut pool_name_len = 0u32;
        oci::session_pool_create(
            env.as_ref(), &err, &pool,
            &mut pool_name_ptr, &mut pool_name_len,
            dbname.as_ptr(), dbname.len() as u32,
            min as u32, max as u32, inc as u32,
            username.as_ptr(), username.len() as u32,
            password.as_ptr(), password.len() as u32,
            OCI_SPC_HOMOGENEOUS | OCI_SPC_STMTCACHE
        )?;
        let name = unsafe { std::slice::from_raw_parts(pool_name_ptr, pool_name_len as usize) };
        Ok(Self {env: env.get_env(), err, pool, name, phantom_env: PhantomData})
    }

    pub(crate) fn get_svc_ctx(&self) -> Result<Ptr<OCISvcCtx>> {
        let inf = Handle::<OCIAuthInfo>::new(self.env.as_ref())?;
        let mut svc = Ptr::<OCISvcCtx>::null();
        let mut found = 0u8;
        oci::session_get(
            self.env.as_ref(), &self.err, svc.as_mut_ptr(), &inf,
            self.name.as_ptr(), self.name.len() as u32, &mut found,
            OCI_SESSGET_SPOOL | OCI_SESSGET_PURITY_SELF
        )?;
        Ok(svc)
    }

    /**
        Returns a new session with a new underlyng connection from this pool.

        # Example

        ```
        use sibyl::{Environment, Connection, Date, Result};

        fn main() -> Result<()> {
            use std::{env, thread, sync::Arc};
            use once_cell::sync::OnceCell;

            static ORACLE : OnceCell<Environment> = OnceCell::new();
            let oracle = ORACLE.get_or_try_init(|| {
                Environment::new()
            })?;

            let dbname = env::var("DBNAME").expect("database name");
            let dbuser = env::var("DBUSER").expect("user name");
            let dbpass = env::var("DBPASS").expect("password");

            let pool = oracle.create_session_pool(&dbname, &dbuser, &dbpass, 0, 1, 3)?;
            let pool = Arc::new(pool);

            let mut workers = Vec::with_capacity(10);
            while workers.len() < 10 {
                let pool = pool.clone();
                let handle = thread::spawn(move || -> String {

                    let conn = pool.get_session().expect("database session");

                    select_latest_hire(&conn).expect("selected employee name")
                });
                workers.push(handle);
            }
            for handle in workers {
                let name = handle.join().expect("select result");
                assert_eq!(name, "Amit Banda was hired on April 21, 2008");
            }
            Ok(())
        }
        # fn select_latest_hire(conn: &Connection) -> Result<String> {
        #     let stmt = conn.prepare("
        #         SELECT first_name, last_name, hire_date
        #           FROM (
        #                 SELECT first_name, last_name, hire_date
        #                      , Row_Number() OVER (ORDER BY hire_date DESC, last_name) AS hire_date_rank
        #                   FROM hr.employees
        #                )
        #          WHERE hire_date_rank = 1
        #     ")?;
        #     let rows = stmt.query(())?;
        #     if let Some( row ) = rows.next()? {
        #         let first_name : Option<&str> = row.get(0)?;
        #         let last_name : &str = row.get(1)?.expect("last_name");
        #         let name = first_name.map_or(last_name.to_string(), |first_name| format!("{} {}", first_name, last_name));
        #         let hire_date : Date = row.get(2)?.expect("hire_date");
        #         let hire_date = hire_date.to_string("FMMonth DD, YYYY")?;
        #         Ok(format!("{} was hired on {}", name, hire_date))
        #     } else {
        #         Ok("Not found".to_string())
        #     }
        # }
        ```
    */
    pub fn get_session(&self) -> Result<Connection> {
        Connection::from_session_pool(self)
    }
}
