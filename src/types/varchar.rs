//! Fixed or variable-length string

mod tosql;

use crate::{
    Result, catch,
    oci::{ *, ptr::Ptr },
    env::Env,
};

pub(crate) fn new(size: u32, env: *mut OCIEnv, err: *mut OCIError) -> Result<Ptr<OCIString>> {
    let txt = Ptr::null();
    catch!{err =>
        OCIStringResize(env, err, size, txt.as_ptr())
    }
    Ok( txt )
}

pub(crate) fn free(txt: &Ptr<OCIString>, env: *mut OCIEnv, err: *mut OCIError) {
    unsafe {
        OCIStringResize(env, err, 0, txt.as_ptr());
    }
}

pub(crate) fn capacity(txt: *const OCIString, env: *mut OCIEnv, err: *mut OCIError) -> Result<usize> {
    let mut size = 0u32;
    catch!{err =>
        OCIStringAllocSize(env, err, txt, &mut size)
    }
    Ok( size as usize )
}

pub(crate) fn to_string(txt: *const OCIString, env: *mut OCIEnv) -> String {
    let txt = unsafe {
        let ptr = OCIStringPtr(env, txt);
        let len = OCIStringSize(env, txt) as usize;
        std::slice::from_raw_parts(ptr, len)
    };
    String::from_utf8_lossy(txt).to_string()
}

pub(crate) fn raw_ptr(txt: *const OCIString, env: *mut OCIEnv) -> *mut u8 {
    unsafe {
        OCIStringPtr(env, txt)
    }
}

pub(crate) fn len(txt: *const OCIString, env: *mut OCIEnv) -> usize {
    unsafe {
        OCIStringSize(env, txt) as usize
    }
}

pub(crate) fn as_str<'a>(txt: *const OCIString, env: *mut OCIEnv) -> &'a str {
    unsafe {
        std::str::from_utf8_unchecked(
            std::slice::from_raw_parts(
                raw_ptr(txt, env),
                len(txt, env)
            )
        )
    }
}

/// Represents Oracle character types - VARCHAR, LONG, etc.
pub struct Varchar<'a> {
    txt: Ptr<OCIString>,
    env: &'a dyn Env,
}

impl Drop for Varchar<'_> {
    fn drop(&mut self) {
        free(&self.txt, self.env.env_ptr(), self.env.err_ptr());
    }
}

impl<'a> Varchar<'a> {
    /**
        Returns a new Varchar constructed from the specified string

        # Example
        ```
        use sibyl::{ self as oracle, Varchar };
        let env = oracle::env()?;

        let txt = Varchar::from("Hello, World!", &env)?;

        assert!(txt.capacity()? >= 13);
        assert_eq!(txt.len(), 13);
        assert_eq!(txt.as_str(), "Hello, World!");
        # Ok::<(),oracle::Error>(())
        ```
    */
    pub fn from(text: &str, env: &'a dyn Env) -> Result<Self> {
        let txt = Ptr::null();
        catch!{env.err_ptr() =>
            OCIStringAssignText(env.env_ptr(), env.err_ptr(), text.as_ptr(), text.len() as u32, txt.as_ptr())
        }
        Ok( Self { env, txt } )
    }

    /**
        Returns a new Varchar constructed with the copy of the date from the `other` Varchar.

        # Example
        ```
        use sibyl::{ self as oracle, Varchar };
        let env = oracle::env()?;

        let src = Varchar::from("Hello, World!", &env)?;
        let txt = Varchar::from_varchar(&src)?;

        assert_eq!(txt.len(), 13);
        assert_eq!(txt.as_str(), "Hello, World!");
        # Ok::<(),oracle::Error>(())
        ```
    */
    pub fn from_varchar(other: &'a Varchar) -> Result<Self> {
        let env = other.env;
        let txt = Ptr::null();
        catch!{env.err_ptr() =>
            OCIStringAssign(env.env_ptr(), env.err_ptr(), other.as_ptr(), txt.as_ptr())
        }
        Ok( Self { env, txt } )
    }

    pub(crate) fn from_ocistring(oci_str: *const OCIString, env: &'a dyn Env) -> Result<Self> {
        let txt = Ptr::null();
        catch!{env.err_ptr() =>
            OCIStringAssign(env.env_ptr(), env.err_ptr(), oci_str, txt.as_ptr())
        }
        Ok( Self { env, txt } )
    }

    /**
        Returns a new Varchar with the memory allocated for the txt data.

        # Example
        ```
        use sibyl::{ self as oracle, Varchar };
        let env = oracle::env()?;

        let txt = Varchar::with_capacity(19, &env)?;

        assert!(txt.capacity()? >= 19);
        assert_eq!(txt.len(), 0);
        # Ok::<(),oracle::Error>(())
        ```
    */
    pub fn with_capacity(size: usize, env: &'a dyn Env) -> Result<Self> {
        let txt = new(size as u32, env.env_ptr(), env.err_ptr())?;
        Ok( Self { env, txt } )
    }

    pub(crate) fn as_ptr(&self) -> *const OCIString {
        self.txt.get()
    }

    pub(crate) fn as_mut_ptr(&self) -> *mut OCIString {
        self.txt.get()
    }

    /**
        Sets the content of self to `text`

        # Example
        ```
        use sibyl::{ self as oracle, Varchar };
        let env = oracle::env()?;

        let mut txt = Varchar::with_capacity(0, &env)?;
        txt.set("Hello, World!")?;

        assert_eq!(txt.len(), 13);
        assert_eq!(txt.as_str(), "Hello, World!");
        # Ok::<(),oracle::Error>(())
        ```
    */
    pub fn set(&mut self, text: &str) -> Result<()> {
        catch!{self.env.err_ptr() =>
            OCIStringAssignText(self.env.env_ptr(), self.env.err_ptr(), text.as_ptr(), text.len() as u32, self.txt.as_ptr())
        }
        Ok(())
    }

    /**
        Returns the size of the string in bytes.

        # Example
        ```
        use sibyl::{ self as oracle, Varchar };
        let env = oracle::env()?;

        let mut txt = Varchar::from("🚲🛠📬🎓", &env)?;

        assert_eq!(txt.len(), 16);
        # Ok::<(),oracle::Error>(())
        ```
    */
    pub fn len(&self) -> usize {
        len(self.as_ptr(), self.env.env_ptr())
    }

    /**
        Returns the allocated size of string memory in bytes

        # Example
        ```
        use sibyl::{ self as oracle, Varchar };
        let env = oracle::env()?;

        let mut txt = Varchar::from("🚲🛠📬🎓", &env)?;

        assert!(txt.capacity()? >= 16);
        # Ok::<(),oracle::Error>(())
        ```
    */
    pub fn capacity(&self) -> Result<usize> {
        capacity(self.as_ptr(), self.env.env_ptr(), self.env.err_ptr())
    }

    /**
        Changes the size of the memory of a string in the object cache.
        Content of the string is not preserved.

        # Example
        ```
        use sibyl::{ self as oracle, Varchar };
        let env = oracle::env()?;

        let mut txt = Varchar::with_capacity(10, &env)?;
        assert!(txt.capacity()? >= 10);

        txt.resize(20)?;
        assert!(txt.capacity()? >= 20);

        txt.resize(0)?;
        // Cannot not ask for capacity after resize to 0.
        // Yes, it works for Raw, but not for Varchars
        let res = txt.capacity();
        assert!(res.is_err());
        if let Err( sibyl::Error::Oracle(code, _message) ) = res {
            assert_eq!(code, 21500);
        } else {
            panic!("cannot match the error");
        }

        txt.resize(16)?;
        assert!(txt.capacity()? >= 16);
        # Ok::<(),oracle::Error>(())
        ```
    */
    pub fn resize(&mut self, new_size: usize) -> Result<()> {
        catch!{self.env.err_ptr() =>
            OCIStringResize(self.env.env_ptr(), self.env.err_ptr(), new_size as u32, self.txt.as_ptr())
        }
        Ok(())
    }

    /**
        Extracts a string slice containing the entire content of the VARCHAR.

        # Example
        ```
        use sibyl::{ self as oracle, Varchar };
        let env = oracle::env()?;

        let txt = Varchar::from("Hello, World!", &env)?;

        assert_eq!(txt.as_str(), "Hello, World!");
        # Ok::<(),oracle::Error>(())
        ```
    */
    pub fn as_str(&self) -> &str {
        as_str(self.as_ptr(), self.env.env_ptr())
    }

    /// Returns unsafe pointer to the string data
    pub fn as_raw_ptr(&self) -> *mut u8 {
        raw_ptr(self.as_ptr(), self.env.env_ptr())
    }
}

impl std::fmt::Debug for Varchar<'_> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        const MAX_LEN : usize = 50;
        let len = self.len();
        if len == 0 {
            f.write_str("VARCHAR ''")
        } else if len <= MAX_LEN {
            f.write_fmt(format_args!("VARCHAR '{}'", self.as_str()))
        } else {
            f.write_fmt(format_args!("VARCHAR '{}...'", &self.as_str()[..MAX_LEN]))
        }
    }
}

impl std::string::ToString for Varchar<'_> {
    fn to_string(&self) -> String {
        to_string(self.as_ptr(), self.env.env_ptr())
    }
}
