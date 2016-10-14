error_chain! {

    errors {
        Exists(oid: [u8; 8]) {
            description("ZODB.POSException.Exists")
                display("The oid, {:?}, exists, but tried to store as new", oid)
        }
        
        POSKeyError(oid: [u8; 8]) {
            description("ZODB.POSException.POSKeyError")
            display("No such oid: {:?}", oid)
        }
    }
}
