#[cfg(feature="blocking")]
mod tests {
    use once_cell::sync::OnceCell;
    use sibyl::*;

    static ORACLE : OnceCell<Environment> = OnceCell::new();
    static POOL : OnceCell<SessionPool> = OnceCell::new();

    fn get_session() -> Result<Session<'static>> {
        let dbname = std::env::var("DBNAME").expect("database name");
        let dbuser = std::env::var("DBUSER").expect("user name");
        let dbpass = std::env::var("DBPASS").expect("password");

        let oracle = ORACLE.get_or_try_init(|| {
            Environment::new()
        })?;
        let pool = POOL.get_or_try_init(|| {
            oracle.create_session_pool(&dbname, &dbuser, &dbpass, 0, 1, 10)
        })?;
        pool.get_session()
    }

    #[test]
    fn num_values() -> Result<()> {
        let session = get_session()?;

        let stmt = session.prepare("SELECT Nvl(:val,42) FROM dual")?;
        stmt.set_prefetch_rows(1)?;

        let arg : Option<i32> = None;
        let row = stmt.query_single(arg)?.unwrap();
        let val : i32 = row.get(0)?;
        assert_eq!(val, 42);

        let arg : Option<i32> = None;
        let row = stmt.query_single(&arg)?.unwrap();
        let val : i32 = row.get(0)?;
        assert_eq!(val, 42);

        let arg : Option<&i32> = None;
        let row = stmt.query_single(arg)?.unwrap();
        let val : i32 = row.get(0)?;
        assert_eq!(val, 42);

        let arg : Option<&i32> = None;
        let row = stmt.query_single(&arg)?.unwrap();
        let val : i32 = row.get(0)?;
        assert_eq!(val, 42);

        let arg = Some(99);
        let row = stmt.query_single(arg)?.unwrap();
        let val : i32 = row.get(0)?;
        assert_eq!(val, 99);

        let arg = Some(99);
        let row = stmt.query_single(&arg)?.unwrap();
        let val : i32 = row.get(0)?;
        assert_eq!(val, 99);

        let num = 99;
        let arg = Some(&num);
        let row = stmt.query_single(arg)?.unwrap();
        let val : i32 = row.get(0)?;
        assert_eq!(val, 99);

        let arg = Some(&num);
        let row = stmt.query_single(&arg)?.unwrap();
        let val : i32 = row.get(0)?;
        assert_eq!(val, 99);

        let stmt = session.prepare("
        BEGIN
            :VAL := Nvl(:VAL, 0) + 1;
        END;
        ")?;
        let mut val : Option<i32> = None;
        let count = stmt.execute(&mut val)?;
        assert_eq!(count, 1);
        assert_eq!(val, Some(1));

        let mut val = Some(99);
        let count = stmt.execute(&mut val)?;
        assert_eq!(count, 1);
        assert_eq!(val, Some(100));

        let val = 99;
        let mut arg = Some(&val);
        let count = stmt.execute(&mut arg)?;
        assert_eq!(count, 1);

        #[cfg(not(feature="unsafe-direct-binds"))]
        assert_eq!(val, 99);
        #[cfg(feature="unsafe-direct-binds")]
        // val's memory is bound to :VAL and thus OCI reads from it
        // directly, but it also writes back into it as :VAL is also an OUT.
        // The user must either not make these mistakes - binding read-only
        // variable to an OUT parameter - or use default "safe binds", where
        // the value is first copied into a buffer.
        // BTW, if the val was also in a read-only section, like literal str
        // for example, then during `execute` program would fail with SIGSEGV,
        // when OCI would try to write into the bound memory.
        assert_eq!(val, 100);

        let mut val = 99;
        let mut arg = Some(&mut val);
        let count = stmt.execute(&mut arg)?;
        assert_eq!(count, 1);
        assert_eq!(val, 100);

        let stmt = session.prepare("
        BEGIN
            :VAL := NULL;
        END;
        ")?;
        let mut val = Some(99);
        let count = stmt.execute(&mut val)?;
        assert_eq!(count, 1);
        assert!(val.is_none());

        Ok(())
    }

    #[test]
    // Unlike Option that owns the value, where new value can be inserted,
    // Option-al ref cannot be changed, thus we always get ORA-06502 - buffer too small -
    // as the "buffer" here has literal length of 0 bytes.
    fn output_to_none() -> Result<()> {
        let session = get_session()?;

        let stmt = session.prepare("
        BEGIN
            :VAL := Nvl(:VAL, 0) + 1;
        END;
        ")?;
        let mut val : Option<&i32> = None;
        let res = stmt.execute(&mut val);
        match res {
            Err(Error::Oracle(code, _)) => {
                assert_eq!(code, 6502);
            },
            _ => {
                panic!("unexpected result");
            }
        }

        let mut val : Option<&mut i32> = None;
        let res = stmt.execute(&mut val);
        match res {
            Err(Error::Oracle(code, _)) => {
                assert_eq!(code, 6502);
            },
            _ => {
                panic!("unexpected result");
            }
        }

        let stmt = session.prepare("
        BEGIN
            :VAL := 'area 51';
        END;
        ")?;
        let mut val : Option<&str> = None;
        let res = stmt.execute(&mut val);
        match res {
            Err(Error::Oracle(code, _)) => {
                assert_eq!(code, 6502);
            },
            _ => {
                panic!("unexpected result");
            }
        }

        Ok(())
    }

    #[test]
    fn str_slices() -> Result<()> {
        let session = get_session()?;

        let stmt = session.prepare("SELECT Nvl(:val,'None') FROM dual")?;
        stmt.set_prefetch_rows(1)?;

        let arg : Option<&str> = None;
        let row = stmt.query_single(arg)?.unwrap();
        let val : &str = row.get(0)?;
        assert_eq!(val, "None");

        let arg : Option<&str> = None;
        let row = stmt.query_single(&arg)?.unwrap();
        let val : &str = row.get(0)?;
        assert_eq!(val, "None");

        let arg : Option<&&str> = None;
        let row = stmt.query_single(arg)?.unwrap();
        let val : &str = row.get(0)?;
        assert_eq!(val, "None");

        let arg : Option<&&str> = None;
        let row = stmt.query_single(&arg)?.unwrap();
        let val : &str = row.get(0)?;
        assert_eq!(val, "None");

        let arg = Some("Text");
        let row = stmt.query_single(arg)?.unwrap();
        let val : &str = row.get(0)?;
        assert_eq!(val, "Text");

        let arg = Some("Text");
        let row = stmt.query_single(&arg)?.unwrap();
        let val : &str = row.get(0)?;
        assert_eq!(val, "Text");

        let txt = "Text";
        let arg = Some(&txt);
        let row = stmt.query_single(arg)?.unwrap();
        let val : &str = row.get(0)?;
        assert_eq!(val, "Text");

        let arg = Some(&txt);
        let row = stmt.query_single(&arg)?.unwrap();
        let val : &str = row.get(0)?;
        assert_eq!(val, "Text");

        let stmt = session.prepare("
        BEGIN
            :VAL := 'area 51';
        END;
        ")?;
        // Start with a String becuase we need str in a writable section for unsafe-direct-binds
        // variant of this test. If we use literal str, it'll be placed into .rodata, and during
        // stmt.execute the app will get SIGSEGV.
        let txt = String::from("unknown");
        let txt = txt.as_str();
        let val = Some(&txt);
        let cnt = stmt.execute(val)?;
        assert_eq!(cnt, 1);

        #[cfg(not(feature="unsafe-direct-binds"))]
        assert_eq!(txt, "unknown");
        #[cfg(feature="unsafe-direct-binds")]
        assert_eq!(txt, "area 51");

        let stmt = session.prepare("
        BEGIN
            :VAL := NULL;
        END;
        ")?;
        let mut val = Some("text");
        let cnt = stmt.execute(&mut val)?;
        assert_eq!(cnt, 1);
        assert!(val.is_none());

        Ok(())
    }

    #[test]
    fn strings() -> Result<()> {
        let session = get_session()?;

        let stmt = session.prepare("SELECT Nvl(:val,'None') FROM dual")?;
        stmt.set_prefetch_rows(1)?;

        let arg : Option<String> = None;
        let row = stmt.query_single(arg)?.unwrap();
        let val : &str = row.get(0)?;
        assert_eq!(val, "None");

        let arg : Option<String> = None;
        let row = stmt.query_single(&arg)?.unwrap();
        let val : &str = row.get(0)?;
        assert_eq!(val, "None");

        let arg : Option<&String> = None;
        let row = stmt.query_single(arg)?.unwrap();
        let val : &str = row.get(0)?;
        assert_eq!(val, "None");

        let arg : Option<&String> = None;
        let row = stmt.query_single(&arg)?.unwrap();
        let val : &str = row.get(0)?;
        assert_eq!(val, "None");

        let arg = Some(String::from("Text"));
        let row = stmt.query_single(arg)?.unwrap();
        let val : &str = row.get(0)?;
        assert_eq!(val, "Text");

        let arg = Some(String::from("Text"));
        let row = stmt.query_single(&arg)?.unwrap();
        let val : &str = row.get(0)?;
        assert_eq!(val, "Text");

        let txt = String::from("Text");
        let arg = Some(&txt);
        let row = stmt.query_single(arg)?.unwrap();
        let val : &str = row.get(0)?;
        assert_eq!(val, "Text");

        let arg = Some(&txt);
        let row = stmt.query_single(&arg)?.unwrap();
        let val : &str = row.get(0)?;
        assert_eq!(val, "Text");

        let stmt = session.prepare("
        BEGIN
            IF :VAL IS NULL THEN
                :VAL := 'Area 51';
            ELSE
                :VAL := '<<' || :VAL || '>>';
            END IF;
        END;
        ")?;
        let mut val : Option<String> = None;
        let cnt = stmt.execute(&mut val)?;
        assert_eq!(cnt, 1);
        assert_eq!(val, Some(String::from("Area 51")));

        let res = stmt.execute(&mut val);
        // There is no space in the updated `val` for "<<" and ">>"
        match res {
            Err(Error::Oracle(code, _)) => {
                assert_eq!(code, 6502);
            },
            _ => {
                panic!("unexpected result");
            }
        }

        let mut txt = String::with_capacity(16);
        txt.push_str("Area 51");
        let mut val = Some(txt);
        let cnt = stmt.execute(&mut val)?;
        assert!(cnt > 0);
        assert_eq!(val, Some(String::from("<<Area 51>>")));

        let stmt = session.prepare("
        BEGIN
            :VAL := NULL;
        END;
        ")?;
        let mut val = Some(String::from("text"));
        let cnt = stmt.execute(&mut val)?;
        assert_eq!(cnt, 1);
        assert!(val.is_none());

        Ok(())
    }

    #[test]
    fn bin_slices() -> Result<()> {
        let session = get_session()?;

        let stmt = session.prepare("SELECT Nvl(:VAL,Utl_Raw.Cast_To_Raw('nil')) FROM dual")?;

        let arg : Option<&[u8]> = None;
        let row = stmt.query_single(&arg)?.unwrap();
        let val : &[u8] = row.get(0)?;
        assert_eq!(val, [0x6e, 0x69, 0x6c]);

        let row = stmt.query_single(arg)?.unwrap();
        let val : &[u8] = row.get(0)?;
        assert_eq!(val, [0x6e, 0x69, 0x6c]);

        let val = [0x62, 0x69, 0x6e].as_ref();
        let arg = Some(val);
        let row = stmt.query_single(&arg)?.unwrap();
        let res : &[u8] = row.get(0)?;
        assert_eq!(res, val);

        let row = stmt.query_single(arg)?.unwrap();
        let res : &[u8] = row.get(0)?;
        assert_eq!(res, val);

        let stmt = session.prepare("
        BEGIN
            :VAL := Utl_Raw.Cast_To_Raw('Area 51');
        END;
        ")?;
        let mut bin = [0;10];
        let mut bin = bin.as_mut();
        let mut val = Some(&mut bin);
        let cnt = stmt.execute(&mut val)?;
        assert_eq!(cnt, 1);
        assert_eq!(bin, [0x41, 0x72, 0x65, 0x61, 0x20, 0x35, 0x31, 0x00, 0x00, 0x00].as_ref());
        // ------- note how it is not very useful as an OUT -------^^^^--^^^^--^^^^

        // However, it is adequate for the "data IN, NULL OUT" use case:
        let stmt = session.prepare("
        BEGIN
            :VAL := NULL;
        END;
        ")?;
        let mut bin = [0x41, 0x72, 0x65, 0x61, 0x20, 0x35, 0x31];
        let mut bin = bin.as_mut();
        let mut val = Some(&mut bin);
        let cnt = stmt.execute(&mut val)?;
        assert_eq!(cnt, 1);
        assert!(val.is_none());

        Ok(())
    }

    #[test]
    fn bin_vec() -> Result<()> {
        let session = get_session()?;

        let stmt = session.prepare("SELECT Nvl(:VAL,Utl_Raw.Cast_To_Raw('nil')) FROM dual")?;

        let arg : Option<Vec<u8>> = None;
        let row = stmt.query_single(&arg)?.unwrap();
        let val : &[u8] = row.get(0)?;
        assert_eq!(val, [0x6e, 0x69, 0x6c]);

        let row = stmt.query_single(arg)?.unwrap();
        let val : &[u8] = row.get(0)?;
        assert_eq!(val, [0x6e, 0x69, 0x6c]);

        let val = [0x62, 0x69, 0x6e].to_vec();
        let arg = Some(val);
        let row = stmt.query_single(&arg)?.unwrap();
        let res : &[u8] = row.get(0)?;
        assert_eq!(res, [0x62, 0x69, 0x6e].as_ref());

        let row = stmt.query_single(arg)?.unwrap();
        let res : &[u8] = row.get(0)?;
        assert_eq!(res, [0x62, 0x69, 0x6e].as_ref());

        let stmt = session.prepare("
        BEGIN
            :VAL := Utl_Raw.Cast_To_Raw('Area 51');
        END;
        ")?;
        let mut bin = Vec::with_capacity(16);
        let mut val = Some(&mut bin);
        let cnt = stmt.execute(&mut val)?;
        assert_eq!(cnt, 1);
        // Unlike &[u8] above Vec is updated to reflect the returned data.
        assert_eq!(bin, [0x41, 0x72, 0x65, 0x61, 0x20, 0x35, 0x31].as_ref());

        // Even more interesting case - NULL IN, data OUT
        // Note that for this to work Option must own Vec, rather than a ref
        let mut val : Option<Vec<u8>> = None;
        let cnt = stmt.execute(&mut val)?;
        assert_eq!(cnt, 1);
        assert!(val.is_some());
        let bin = val.unwrap();
        assert_eq!(bin, [0x41, 0x72, 0x65, 0x61, 0x20, 0x35, 0x31].as_ref());

        let stmt = session.prepare("
        BEGIN
            :VAL := NULL;
        END;
        ")?;
        let mut val = Some([0x41, 0x72, 0x65, 0x61, 0x20, 0x35, 0x31].to_vec());
        let cnt = stmt.execute(&mut val)?;
        assert_eq!(cnt, 1);
        assert!(val.is_none());

        Ok(())
    }

    #[test]
    fn dates() -> Result<()> {
        let session = get_session()?;

        // NULL IN, DATE OUT... kind of :-)
        let stmt = session.prepare("SELECT Nvl(:VAL,To_Date('1969-07-16 13:32:00','YYYY-MM-DD HH24:MI:SS')) FROM dual")?;
        let arg : Option<Date> = None;
        let row = stmt.query_single(arg)?.unwrap();
        let res: Date = row.get(0)?;
        let expected_date = Date::from_string("1969-07-16 13:32:00", "YYYY-MM-DD HH24:MI:SS", &session)?;
        assert_eq!(res.compare(&expected_date)?, std::cmp::Ordering::Equal);

        // DATE IN, DATE OUT
        let stmt = session.prepare("
        BEGIN
            :VAL := Last_Day(:VAL);
        END;
        ")?;
        let mut val = Some(Date::from_string("1969-07-16 13:32:00", "YYYY-MM-DD HH24:MI:SS", &session)?);
        let cnt = stmt.execute(&mut val)?;
        assert_eq!(cnt, 1);
        let expected_date = Date::from_string("1969-07-31 13:32:00", "YYYY-MM-DD HH24:MI:SS", &session)?;
        assert_eq!(val.unwrap().compare(&expected_date)?, std::cmp::Ordering::Equal);

        // DATE IN, NULL OUT
        let stmt = session.prepare("
        BEGIN
            :VAL := NULL;
        END;
        ")?;
        let mut val = Some(Date::from_string("1969-07-16 13:32:00", "YYYY-MM-DD HH24:MI:SS", &session)?);
        let cnt = stmt.execute(&mut val)?;
        assert_eq!(cnt, 1);
        assert!(val.is_none());

        Ok(())
    }

    #[test]
    fn intervals() -> Result<()> {
        let session = get_session()?;

        // NULL IN, INTERVAL OUT... kind of :-)
        let stmt = session.prepare("
            SELECT Nvl(:VAL, To_Timestamp('1969-07-24 16:50:35','YYYY-MM-DD HH24:MI:SS') - To_Timestamp('1969-07-16 13:32:00','YYYY-MM-DD HH24:MI:SS'))
              FROM dual
        ")?;
        let arg : Option<IntervalDS> = None;
        let row = stmt.query_single(arg)?.unwrap();
        let res: IntervalDS = row.get(0)?;
        let expected_interval = IntervalDS::from_string("+8 03:18:35.00", &session)?;
        assert_eq!(res.compare(&expected_interval)?, std::cmp::Ordering::Equal);

        // INTERVAL IN, INTERVAL OUT
        let stmt = session.prepare("
        BEGIN
            :VAL := To_Timestamp('1969-07-20','YYYY-MM-DD') + :VAL - To_Timestamp('1969-07-16 13:32:00','YYYY-MM-DD HH24:MI:SS');
        END;
        ")?;
        let mut val = Some(IntervalDS::from_string("+4 16:50:35.00", &session)?);
        let cnt = stmt.execute(&mut val)?;
        assert_eq!(cnt, 1);
        let expected_interval = IntervalDS::from_string("+8 03:18:35.00", &session)?;
        assert_eq!(val.unwrap().compare(&expected_interval)?, std::cmp::Ordering::Equal);

        // INTERVAL IN, NULL OUT
        let stmt = session.prepare("
        BEGIN
            :VAL := NULL;
        END;
        ")?;
        let mut val = Some(IntervalDS::from_string("+4 16:50:35.00", &session)?);
        let cnt = stmt.execute(&mut val)?;
        assert_eq!(cnt, 1);
        assert!(val.is_none());

        Ok(())
    }

    #[test]
    fn timestamps() -> Result<()> {
        let session = get_session()?;

        // NULL IN, TIMESTAMP OUT... kind of :-)
        let stmt = session.prepare("
            SELECT Nvl(:VAL, To_Timestamp('1969-07-24 16:50:35','YYYY-MM-DD HH24:MI:SS'))
              FROM dual
        ")?;
        let arg : Option<Timestamp> = None;
        let row = stmt.query_single(arg)?.unwrap();
        let res: Timestamp = row.get(0)?;
        let expected_timestamp = Timestamp::from_string("1969-07-24 16:50:35", "YYYY-MM-DD HH24:MI:SS", &session)?;
        assert_eq!(res.compare(&expected_timestamp)?, std::cmp::Ordering::Equal);

        // TIMESTAMP IN, TIMESTAMP OUT
        let stmt = session.prepare("
        BEGIN
            :VAL := :VAL + To_DSInterval('+8 03:18:35.00');
        END;
        ")?;
        let mut val = Some(Timestamp::from_string("1969-07-16 13:32:00", "YYYY-MM-DD HH24:MI:SS", &session)?);
        let cnt = stmt.execute(&mut val)?;
        assert_eq!(cnt, 1);
        let expected_timestamp = Timestamp::from_string("1969-07-24 16:50:35", "YYYY-MM-DD HH24:MI:SS", &session)?;
        assert_eq!(res.compare(&expected_timestamp)?, std::cmp::Ordering::Equal);

        // TIMESTAMP IN, NULL OUT
        let stmt = session.prepare("
        BEGIN
            :VAL := NULL;
        END;
        ")?;
        let mut val = Some(Timestamp::from_string("1969-07-16 13:32:00", "YYYY-MM-DD HH24:MI:SS", &session)?);
        let cnt = stmt.execute(&mut val)?;
        assert_eq!(cnt, 1);
        assert!(val.is_none());

        Ok(())
    }

    #[test]
    fn numbers() -> Result<()> {
        let session = get_session()?;

        // NULL IN, NUMBER OUT (kind of)
        let stmt = session.prepare("
            SELECT Nvl(:VAL, 42) FROM dual
        ")?;
        let arg : Option<Number> = None;
        let row = stmt.query_single(arg)?.unwrap();
        let res: Number = row.get(0)?;
        let expected_number = Number::from_int(42, &session)?;
        assert_eq!(res.compare(&expected_number)?, std::cmp::Ordering::Equal);

        // NUMBER IN, NUMBER OUT
        let stmt = session.prepare("
        BEGIN
            :VAL := :VAL + 1;
        END;
        ")?;
        let mut val = Some(Number::from_int(99, &session)?);
        let cnt = stmt.execute(&mut val)?;
        assert_eq!(cnt, 1);
        let expected_number = Number::from_int(100, &session)?;
        assert!(val.is_some());
        assert_eq!(val.unwrap().compare(&expected_number)?, std::cmp::Ordering::Equal);

        // NUMBER IN, NUMBER OUT
        let stmt = session.prepare("
        BEGIN
            :VAL := NULL;
        END;
        ")?;
        let mut val = Some(Number::from_int(99, &session)?);
        let cnt = stmt.execute(&mut val)?;
        assert_eq!(cnt, 1);
        assert!(val.is_none());

        Ok(())
    }

    #[test]
    fn varchars() -> Result<()> {
        let session = get_session()?;

        // NULL IN, VARCHAR OUT (kind of)
        let stmt = session.prepare("
            SELECT Nvl(:VAL, 'hello') FROM dual
        ")?;
        let arg : Option<Varchar> = None;
        let row = stmt.query_single(arg)?.unwrap();
        let res: Varchar = row.get(0)?;
        assert_eq!(res.as_str(), "hello");

        // VARCHAR IN, VARCHAR OUT
        let stmt = session.prepare("
        BEGIN
            :VAL := '<' || :VAL || '>';
        END;
        ")?;
        let mut txt = Varchar::with_capacity(8, &session)?;
        txt.set("text")?;
        let mut val = Some(txt);
        let cnt = stmt.execute(&mut val)?;
        assert_eq!(cnt, 1);
        assert!(val.is_some());
        assert_eq!(val.unwrap().as_str(), "<text>");

        // VARCHAR IN, NULL OUT
        let stmt = session.prepare("
        BEGIN
            :VAL := NULL;
        END;
        ")?;
        let mut val = Some(Varchar::from("text", &session)?);
        let cnt = stmt.execute(&mut val)?;
        assert_eq!(cnt, 1);
        assert!(val.is_none());

        Ok(())
    }

    #[test]
    fn raws() -> Result<()> {
        let session = get_session()?;

        // NULL IN, RAW (kind of)
        let stmt = session.prepare("SELECT Nvl(:VAL,Utl_Raw.Cast_To_Raw('nil')) FROM dual")?;

        let arg : Option<Raw> = None;
        let row = stmt.query_single(arg)?.unwrap();
        let val : Raw = row.get(0)?;
        assert_eq!(val.as_bytes(), &[0x6e, 0x69, 0x6c]);

        // RAW IN, RAW OUT
        let stmt = session.prepare("
        BEGIN
            :VAL := Utl_Raw.Concat(
                Utl_Raw.Cast_To_Raw('<'),
                :VAL,
                Utl_Raw.Cast_To_Raw('>')
            );
        END;
        ")?;
        let mut bin = Raw::with_capacity(8, &session)?;
        bin.set(&[0x64, 0x61, 0x74, 0x61])?;
        let mut val = Some(bin);
        let cnt = stmt.execute(&mut val)?;
        assert_eq!(cnt, 1);
        assert!(val.is_some());
        assert_eq!(val.unwrap().as_bytes(), &[0x3c, 0x64, 0x61, 0x74, 0x61, 0x3e]);

        // RAW IN, NULL OUT
        let stmt = session.prepare("
        BEGIN
            :VAL := NULL;
        END;
        ")?;
        let mut val = Some(Raw::from_bytes(&[0x64, 0x61, 0x74, 0x61], &session)?);
        let cnt = stmt.execute(&mut val)?;
        assert_eq!(cnt, 1);
        assert!(val.is_none());

        Ok(())
    }
}
