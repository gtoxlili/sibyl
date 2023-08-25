use sibyl::{FromSql, Row};

struct B;

impl<'a> FromSql<'a> for B {

    fn value(row: &'a Row<'a>, col: &mut sibyl::Column) -> sibyl::Result<Self> {
        match col.data() {
            sibyl::ColumnBuffer::Text( oci_str_ptr )   => Ok( varchar::to_string(oci_str_ptr, row.as_ref()) ),
            ColumnBuffer::Number( oci_num_box ) => number::to_string("TM", oci_num_box.as_ref(), row.as_ref()),
            ColumnBuffer::Date( oci_date )      => date::to_string("YYYY-MM-DD HH24::MI:SS", oci_date, row.as_ref()),
            ColumnBuffer::Timestamp( ts )       => timestamp::to_string("YYYY-MM-DD HH24:MI:SSXFF", 3, ts.as_ref(), row),
            ColumnBuffer::TimestampTZ( ts )     => timestamp::to_string("YYYY-MM-DD HH24:MI:SSXFF TZH:TZM", 3, ts.as_ref(), row),
            ColumnBuffer::TimestampLTZ( ts )    => timestamp::to_string("YYYY-MM-DD HH24:MI:SSXFF TZH:TZM", 3, ts.as_ref(), row),
            ColumnBuffer::IntervalYM( int )     => interval::to_string(int.as_ref(), 4, 3, row),
            ColumnBuffer::IntervalDS( int )     => interval::to_string(int.as_ref(), 9, 5, row),
            ColumnBuffer::Float( val )          => Ok( val.to_string() ),
            ColumnBuffer::Double( val )         => Ok( val.to_string() ),
            ColumnBuffer::Rowid( rowid )        => rowid::to_string(rowid, row.as_ref()),
            _                                   => Err( Error::new("cannot return as a String") )
        }
    }
}

fn main() {
    println!("Hello, world!");
}
