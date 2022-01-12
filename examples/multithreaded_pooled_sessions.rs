use sibyl::*;
/**
    This example is a variant of `readme` that executes its work in multiple
    threads (or async tasks). It creates a session pool which threads (or
    tasks) then use to "borrow" stateless sessions to execute queries.

    *Note* that `block_on` used in nonblocking version of this example
    abstracts `block_on` for various async executors and is only intended to
    execute Sibyl's async tests and examples. While you can certainly
    use it, most likely you'd want to create your own version of it.
*/
fn main() -> Result<()> {
    example()
}

#[cfg(feature="blocking")]
fn example() -> Result<()> {
    use std::{env, thread, sync::Arc};
    use once_cell::sync::OnceCell;

    static ORACLE : OnceCell<Environment> = OnceCell::new();
    let oracle = ORACLE.get_or_try_init(|| {
        env()
    })?;

    let dbname = env::var("DBNAME").expect("database name");
    let dbuser = env::var("DBUSER").expect("user name");
    let dbpass = env::var("DBPASS").expect("password");

    let pool = oracle.create_session_pool(&dbname, &dbuser, &dbpass, 0, 1, 10)?;
    let pool = Arc::new(pool);

    let mut workers = Vec::with_capacity(98);
    for _i in 0..workers.capacity() {
        let pool = pool.clone();
        let handle = thread::spawn(move || -> Result<Option<(String,String)>> {
            let session = pool.get_session()?;
            let stmt = session.prepare("
                SELECT first_name, last_name, hire_date
                  FROM (
                        SELECT first_name, last_name, hire_date
                             , Row_Number() OVER (ORDER BY hire_date DESC, last_name) AS hire_date_rank
                          FROM hr.employees
                       )
                 WHERE hire_date_rank = 1
            ")?;
            let rows = stmt.query(())?;
            if let Some( row ) = rows.next()? {
                let first_name : Option<&str> = row.get(0)?;
                let last_name : &str = row.get(1)?.unwrap();
                let name = first_name.map_or(last_name.to_string(), |first_name| format!("{} {}", first_name, last_name));
                let hire_date : Date = row.get(2)?.unwrap();
                let hire_date = hire_date.to_string("FMMonth DD, YYYY")?;

                Ok(Some((name, hire_date)))
            } else {
                Ok(None)
            }
        });
        workers.push(handle);
    }
    for handle in workers {
        let worker_id = handle.thread().id();
        if let Some((name,hire_date)) = handle.join().expect("result from worker thread")? {
            println!("{:?}: {} was hired on {}", worker_id, name, hire_date);
        } else {
            println!("{:?}: did not find the latest hire", worker_id);
        }
    }
    println!("There are {} open sessions in the pool.", pool.open_count()?);
    Ok(())
}

#[cfg(feature="nonblocking")]
fn example() -> Result<()> {
    use std::{env, sync::Arc};
    use once_cell::sync::OnceCell;

    block_on(async {

        static ORACLE : OnceCell<Environment> = OnceCell::new();
        let oracle = ORACLE.get_or_try_init(|| {
            env()
        })?;

        let dbname = env::var("DBNAME").expect("database name");
        let dbuser = env::var("DBUSER").expect("user name");
        let dbpass = env::var("DBPASS").expect("password");

        let pool = oracle.create_session_pool(&dbname, &dbuser, &dbpass, 0, 1, 10).await?;
        let pool = Arc::new(pool);

        let mut workers = Vec::with_capacity(100);
        for _i in 0..workers.capacity() {
            let pool = pool.clone();
            let handle = spawn(async move {
                let session = pool.get_session().await?;
                let stmt = session.prepare("
                    SELECT first_name, last_name, hire_date
                      FROM (
                            SELECT first_name, last_name, hire_date
                                 , Row_Number() OVER (ORDER BY hire_date DESC, last_name) AS hire_date_rank
                              FROM hr.employees
                           )
                     WHERE hire_date_rank = 1
                ").await?;
                let rows = stmt.query(()).await?;
                if let Some( row ) = rows.next().await? {
                    let first_name : Option<&str> = row.get(0)?;
                    let last_name : &str = row.get(1)?.unwrap();
                    let name = first_name.map_or(last_name.to_string(), |first_name| format!("{} {}", first_name, last_name));
                    let hire_date : Date = row.get(2)?.unwrap();
                    let hire_date = hire_date.to_string("FMMonth DD, YYYY")?;

                    Ok::<_,Error>(Some((name, hire_date)))
                } else {
                    Ok(None)
                }
            });
            workers.push(handle);
        }
        let mut n = 1;
        for handle in workers {
            if let Some((name,hire_date)) = handle.await.expect("task's result")? {
                println!("{:?}: {} was hired on {}", n, name, hire_date);
            } else {
                println!("{:?}: did not find the latest hire", n);
            }
            n += 1;
        }
        println!("There are {} open sessions in the pool.", pool.open_count()?);

        Ok(())
    })
}
