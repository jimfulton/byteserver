use byteorder::{ByteOrder, BigEndian};

const SCONV: f64 = 60.0 / (1u64 <<32) as f64;

type Tid = [u8; 8];

pub fn make_tid(year: u32, month: u32, day: u32, hour: u32, minute: u32,
                second: f64)
                -> Tid {

    let days = ((year - 1900) * 12 + month - 1) * 31 + day - 1;
    let minutes = ((days * 24 + hour) * 60 + minute) as u64;
    let seconds = (second / SCONV) as u64;

    let mut tid: Tid = [0u8; 8];
    BigEndian::write_u64(&mut tid, (minutes << 32) + seconds);
    tid
}

pub fn tm_tid(dt: time::OffsetDateTime) -> Tid {
    let year = dt.year() as i32;
    let month = dt.month() as i32;
    let day = dt.day() as i32;
    let hour = dt.hour() as i32;
    let minute = dt.minute() as i32;
    let second = dt.second() as f64 + (dt.nanosecond() as f64 / 1_000_000_000.0);

    let days = ((year - 1900) * 12 + month - 1) * 31 + day - 1;
    let minutes = ((days * 24 + hour) * 60 + minute) as u64;
    let seconds = (second / SCONV) as u64;

    let mut tid: Tid = [0u8; 8];
    BigEndian::write_u64(&mut tid, (minutes << 32) + seconds);
    tid
}

pub fn now_tid() -> Tid { tm_tid(time::OffsetDateTime::now_utc()) }

pub fn next(tid: &Tid) -> Tid {
    let mut next = tid.clone();
    let iold = BigEndian::read_u64(&mut next);
    BigEndian::write_u64(&mut next, iold + 1);
    next
}

pub fn later_than(new: Tid, old: Tid) -> Tid {
    if new > old {
        new
    }
    else {
        next(&old)
    }
}

// ======================================================================

#[cfg(test)]
mod tests {

    use super::*;
    use time;

    #[test]
    fn test_make_tid() {
        assert_eq!(make_tid(2016, 1, 2, 3, 4, 59.99999999999),
                   [3, 180, 48, 88, 255, 255, 255, 255]);
        assert_eq!(make_tid(2016, 1, 2, 3, 4, 56.789),
                   [3, 180, 48, 88, 242, 76, 187, 82]);
    }

    #[test]
    fn test_tm_tid() {
        use time::macros::datetime;
        assert_eq!(
            tm_tid(datetime!(2016-01-02 03:04:59.999_999_999 UTC)),
            [3, 180, 48, 88, 255, 255, 255, 255]);
        assert_eq!(make_tid(2016, 1, 2, 3, 4, 56.789),
                   [3, 180, 48, 88, 242, 76, 187, 82]);
    }

    #[test]
    fn test_later_than() {
    
        assert_eq!(later_than([3, 180, 48, 88, 255, 255, 255, 255],
                              [3, 180, 48, 88, 242, 76, 187, 82]),
                   [3, 180, 48, 88, 255, 255, 255, 255]);
        
        assert_eq!(later_than([3, 180, 48, 88, 242, 76, 187, 82],
                              [3, 180, 48, 88, 255, 255, 255, 255]),
                   [3, 180, 48, 89, 0, 0, 0, 0]);
    }
}
    
