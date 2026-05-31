use thiserror::Error;
use windows::Win32::Networking::WinSock::WSA_ERROR;

/// Generated from WinSock documentation.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Error)]
#[repr(i32)]
pub enum WinsockError {
    /// Specified event object handle is invalid.
    ///
    /// An application attempts to use an event object, but the specified handle is not valid.
    #[error("specified event object handle is invalid")]
    InvalidHandle = 6,

    /// Insufficient memory available.
    ///
    /// An application used a Windows Sockets function that directly maps to a Windows function. The Windows function is indicating a lack of required memory resources.
    #[error("insufficient memory available")]
    NotEnoughMemory = 8,

    /// One or more parameters are invalid.
    ///
    /// An application used a Windows Sockets function which directly maps to a Windows function. The Windows function is indicating a problem with one or more parameters.
    #[error("one or more parameters are invalid")]
    InvalidParameter = 87,

    /// Overlapped operation aborted.
    ///
    /// An overlapped operation was canceled due to the closure of the socket, or the execution of the SIO_FLUSH command in WSAIoctl .
    #[error("overlapped operation aborted")]
    OperationAborted = 995,

    /// Overlapped I/O event object not in signaled state.
    ///
    /// The application has tried to determine the status of an overlapped operation which is not yet completed. Applications that use WSAGetOverlappedResult (with the fWait flag set to FALSE ) in a polling mode to determine when an overlapped operation has completed, get this error code until the operation is complete.
    #[error("overlapped i/o event object not in signaled state")]
    IoIncomplete = 996,

    ///
    ///
    /// The application has initiated an overlapped operation that cannot be completed immediately. A completion indication will be given later when the operation has been completed.
    #[error("")]
    IoPending = 997,

    /// Interrupted function call.
    ///
    /// A blocking operation was interrupted by a call to WSACancelBlockingCall .
    #[error("interrupted function call")]
    Intr = 10004,

    /// File handle is not valid.
    ///
    /// The file handle supplied is not valid.
    #[error("file handle is not valid")]
    Badf = 10009,

    /// Permission denied.
    ///
    /// An attempt was made to access a socket in a way forbidden by its access permissions. An example is using a broadcast address for sendto without broadcast permission being set using setsockopt (SO_BROADCAST). Another possible reason for the WSAEACCES error is that when the bind function is called (on Windows NT 4.0 with SP4 and later), another application, service, or kernel mode driver is bound to the same address with exclusive access. Such exclusive access is a new feature of Windows NT 4.0 with SP4 and later, and is implemented by using the SO_EXCLUSIVEADDRUSE option.
    #[error("permission denied")]
    Acces = 10013,

    /// Bad address.
    ///
    /// The system detected an invalid pointer address in attempting to use a pointer argument of a call. This error occurs if an application passes an invalid pointer value, or if the length of the buffer is too small. For instance, if the length of an argument, which is a sockaddr structure, is smaller than the sizeof(sockaddr).
    #[error("bad address")]
    Fault = 10014,

    /// Invalid argument.
    ///
    /// Some invalid argument was supplied (for example, specifying an invalid level to the setsockopt function). In some instances, it also refers to the current state of the socket—for instance, calling accept on a socket that is not listening.
    #[error("invalid argument")]
    Inval = 10022,

    /// Too many open files.
    ///
    /// Too many open sockets. Each implementation may have a maximum number of socket handles available, either globally, per process, or per thread.
    #[error("too many open files")]
    Mfile = 10024,

    /// Resource temporarily unavailable.
    ///
    /// This error is returned from operations on nonblocking sockets that cannot be completed immediately, for example recv when no data is queued to be read from the socket. It is a nonfatal error, and the operation should be retried later. It is normal for WSAEWOULDBLOCK to be reported as the result from calling connect on a nonblocking SOCK_STREAM socket, since some time must elapse for the connection to be established.
    #[error("resource temporarily unavailable")]
    Wouldblock = 10035,

    /// Operation now in progress.
    ///
    /// A blocking operation is currently executing. Windows Sockets only allows a single blocking operation—per- task or thread—to be outstanding, and if any other function call is made (whether or not it references that or any other socket) the function fails with the WSAEINPROGRESS error.
    #[error("operation now in progress")]
    Inprogress = 10036,

    /// Operation already in progress.
    ///
    /// An operation was attempted on a nonblocking socket with an operation already in progress—that is, calling connect a second time on a nonblocking socket that is already connecting, or canceling an asynchronous request ( WSAAsyncGetXbyY ) that has already been canceled or completed.
    #[error("operation already in progress")]
    Already = 10037,

    /// Socket operation on nonsocket.
    ///
    /// An operation was attempted on something that is not a socket. Either the socket handle parameter did not reference a valid socket, or for select , a member of an fd_set was not valid.
    #[error("socket operation on nonsocket")]
    Notsock = 10038,

    /// Destination address required.
    ///
    /// A required address was omitted from an operation on a socket. For example, this error is returned if sendto is called with the remote address of ADDR_ANY.
    #[error("destination address required")]
    Destaddrreq = 10039,

    /// Message too long.
    ///
    /// A message sent on a datagram socket was larger than the internal message buffer or some other network limit, or the buffer used to receive a datagram was smaller than the datagram itself.
    #[error("message too long")]
    Msgsize = 10040,

    /// Protocol wrong type for socket.
    ///
    /// A protocol was specified in the socket function call that does not support the semantics of the socket type requested. For example, the ARPA Internet UDP protocol cannot be specified with a socket type of SOCK_STREAM.
    #[error("protocol wrong type for socket")]
    Prototype = 10041,

    /// Bad protocol option.
    ///
    /// An unknown, invalid or unsupported option or level was specified in a getsockopt or setsockopt call.
    #[error("bad protocol option")]
    Noprotoopt = 10042,

    /// Protocol not supported.
    ///
    /// The requested protocol has not been configured into the system, or no implementation for it exists. For example, a socket call requests a SOCK_DGRAM socket, but specifies a stream protocol.
    #[error("protocol not supported")]
    Protonosupport = 10043,

    /// Socket type not supported.
    ///
    /// The support for the specified socket type does not exist in this address family. For example, the optional type SOCK_RAW might be selected in a socket call, and the implementation does not support SOCK_RAW sockets at all.
    #[error("socket type not supported")]
    Socktnosupport = 10044,

    /// Operation not supported.
    ///
    /// The attempted operation is not supported for the type of object referenced. Usually this occurs when a socket descriptor to a socket that cannot support this operation is trying to accept a connection on a datagram socket.
    #[error("operation not supported")]
    Opnotsupp = 10045,

    /// Protocol family not supported.
    ///
    /// The protocol family has not been configured into the system or no implementation for it exists. This message has a slightly different meaning from WSAEAFNOSUPPORT. However, it is interchangeable in most cases, and all Windows Sockets functions that return one of these messages also specify WSAEAFNOSUPPORT.
    #[error("protocol family not supported")]
    Pfnosupport = 10046,

    /// Address family not supported by protocol family.
    ///
    /// An address incompatible with the requested protocol was used. All sockets are created with an associated address family (that is, AF_INET for Internet Protocols) and a generic protocol type (that is, SOCK_STREAM). This error is returned if an incorrect protocol is explicitly requested in the socket call, or if an address of the wrong family is used for a socket, for example, in sendto .
    #[error("address family not supported by protocol family")]
    Afnosupport = 10047,

    /// Address already in use.
    ///
    /// Typically, only one usage of each socket address (protocol/IP address/port) is permitted. This error occurs if an application attempts to bind a socket to an IP address/port that has already been used for an existing socket, or a socket that was not closed properly, or one that is still in the process of closing. For server applications that need to bind multiple sockets to the same port number, consider using setsockopt (SO_REUSEADDR). Client applications usually need not call bind at all— connect chooses an unused port automatically. When bind is called with a wildcard address (involving ADDR_ANY), a WSAEADDRINUSE error could be delayed until the specific address is committed. This could happen with a call to another function later, including connect , listen , WSAConnect , or WSAJoinLeaf .
    #[error("address already in use")]
    Addrinuse = 10048,

    /// Cannot assign requested address.
    ///
    /// The requested address is not valid in its context. This normally results from an attempt to bind to an address that is not valid for the local computer. This can also result from connect , sendto , WSAConnect , WSAJoinLeaf , or WSASendTo when the remote address or port is not valid for a remote computer (for example, address or port 0).
    #[error("cannot assign requested address")]
    Addrnotavail = 10049,

    /// Network is down.
    ///
    /// A socket operation encountered a dead network. This could indicate a serious failure of the network system (that is, the protocol stack that the Windows Sockets DLL runs over), the network interface, or the local network itself.
    #[error("network is down")]
    Netdown = 10050,

    /// Network is unreachable.
    ///
    /// A socket operation was attempted to an unreachable network. This usually means the local software knows no route to reach the remote host.
    #[error("network is unreachable")]
    Netunreach = 10051,

    /// Network dropped connection on reset.
    ///
    /// The connection has been broken due to keep-alive activity detecting a failure while the operation was in progress. It can also be returned by setsockopt if an attempt is made to set SO_KEEPALIVE on a connection that has already failed.
    #[error("network dropped connection on reset")]
    Netreset = 10052,

    /// Software caused connection abort.
    ///
    /// An established connection was aborted by the software in your host computer, possibly due to a data transmission time-out or protocol error.
    #[error("software caused connection abort")]
    Connaborted = 10053,

    /// Connection reset by peer.
    ///
    /// An existing connection was forcibly closed by the remote host. This normally results if the peer application on the remote host is suddenly stopped, the host is rebooted, the host or remote network interface is disabled, or the remote host uses a hard close (see setsockopt for more information on the SO_LINGER option on the remote socket). This error may also result if a connection was broken due to keep-alive activity detecting a failure while one or more operations are in progress. Operations that were in progress fail with WSAENETRESET. Subsequent operations fail with WSAECONNRESET.
    #[error("connection reset by peer")]
    Connreset = 10054,

    /// No buffer space available.
    ///
    /// An operation on a socket could not be performed because the system lacked sufficient buffer space or because a queue was full.
    #[error("no buffer space available")]
    Nobufs = 10055,

    /// Socket is already connected.
    ///
    /// A connect request was made on an already-connected socket. Some implementations also return this error if sendto is called on a connected SOCK_DGRAM socket (for SOCK_STREAM sockets, the to parameter in sendto is ignored) although other implementations treat this as a legal occurrence.
    #[error("socket is already connected")]
    Isconn = 10056,

    /// Socket is not connected.
    ///
    /// A request to send or receive data was disallowed because the socket is not connected and (when sending on a datagram socket using sendto ) no address was supplied. Any other type of operation might also return this error—for example, setsockopt setting SO_KEEPALIVE if the connection has been reset.
    #[error("socket is not connected")]
    Notconn = 10057,

    /// Cannot send after socket shutdown.
    ///
    /// A request to send or receive data was disallowed because the socket had already been shut down in that direction with a previous shutdown call. By calling shutdown a partial close of a socket is requested, which is a signal that sending or receiving, or both have been discontinued.
    #[error("cannot send after socket shutdown")]
    Shutdown = 10058,

    /// Too many references.
    ///
    /// Too many references to some kernel object.
    #[error("too many references")]
    Toomanyrefs = 10059,

    /// Connection timed out.
    ///
    /// A connection attempt failed because the connected party did not properly respond after a period of time, or the established connection failed because the connected host has failed to respond.
    #[error("connection timed out")]
    Timedout = 10060,

    /// Connection refused.
    ///
    /// No connection could be made because the target computer actively refused it. This usually results from trying to connect to a service that is inactive on the foreign host—that is, one with no server application running.
    #[error("connection refused")]
    Connrefused = 10061,

    /// Cannot translate name.
    ///
    /// Cannot translate a name.
    #[error("cannot translate name")]
    Loop = 10062,

    /// Name too long.
    ///
    /// A name component or a name was too long.
    #[error("name too long")]
    Nametoolong = 10063,

    /// Host is down.
    ///
    /// A socket operation failed because the destination host is down. A socket operation encountered a dead host. Networking activity on the local host has not been initiated. These conditions are more likely to be indicated by the error WSAETIMEDOUT.
    #[error("host is down")]
    Hostdown = 10064,

    /// No route to host.
    ///
    /// A socket operation was attempted to an unreachable host. See WSAENETUNREACH.
    #[error("no route to host")]
    Hostunreach = 10065,

    /// Directory not empty.
    ///
    /// Cannot remove a directory that is not empty.
    #[error("directory not empty")]
    Notempty = 10066,

    /// Too many processes.
    ///
    /// A Windows Sockets implementation may have a limit on the number of applications that can use it simultaneously. WSAStartup may fail with this error if the limit has been reached.
    #[error("too many processes")]
    Proclim = 10067,

    /// User quota exceeded.
    ///
    /// Ran out of user quota.
    #[error("user quota exceeded")]
    Users = 10068,

    /// Disk quota exceeded.
    ///
    /// Ran out of disk quota.
    #[error("disk quota exceeded")]
    Dquot = 10069,

    /// Stale file handle reference.
    ///
    /// The file handle reference is no longer available.
    #[error("stale file handle reference")]
    Stale = 10070,

    /// Item is remote.
    ///
    /// The item is not available locally.
    #[error("item is remote")]
    Remote = 10071,

    /// Network subsystem is unavailable.
    ///
    /// This error is returned by WSAStartup if the Windows Sockets implementation cannot function at this time because the underlying system it uses to provide network services is currently unavailable. Users should check:
    #[error("network subsystem is unavailable")]
    Sysnotready = 10091,

    /// Winsock.dll version out of range.
    ///
    /// The current Windows Sockets implementation does not support the Windows Sockets specification version requested by the application. Check that no old Windows Sockets DLL files are being accessed.
    #[error("winsock.dll version out of range")]
    Vernotsupported = 10092,

    /// Successful WSAStartup not yet performed.
    ///
    /// Either the application has not called WSAStartup or WSAStartup failed. The application may be accessing a socket that the current active task does not own (that is, trying to share a socket between tasks), or WSACleanup has been called too many times.
    #[error("successful wsastartup not yet performed")]
    Notinitialised = 10093,

    /// Graceful shutdown in progress.
    ///
    /// Returned by WSARecv and WSARecvFrom to indicate that the remote party has initiated a graceful shutdown sequence.
    #[error("graceful shutdown in progress")]
    Discon = 10101,

    /// No more results.
    ///
    /// No more results can be returned by the WSALookupServiceNext function.
    #[error("no more results")]
    Nomore = 10102,

    /// Call has been canceled.
    ///
    /// A call to the WSALookupServiceEnd function was made while this call was still processing. The call has been canceled.
    #[error("call has been canceled")]
    Cancelled = 10103,

    /// Procedure call table is invalid.
    ///
    /// The service provider procedure call table is invalid. A service provider returned a bogus procedure table to Ws2_32.dll. This is usually caused by one or more of the function pointers being NULL .
    #[error("procedure call table is invalid")]
    Invalidproctable = 10104,

    /// Service provider is invalid.
    ///
    /// The requested service provider is invalid. This error is returned by the WSCGetProviderInfo and WSCGetProviderInfo32 functions if the protocol entry specified could not be found. This error is also returned if the service provider returned a version number other than 2.0.
    #[error("service provider is invalid")]
    Invalidprovider = 10105,

    /// Service provider failed to initialize.
    ///
    /// The requested service provider could not be loaded or initialized. This error is returned if either a service provider's DLL could not be loaded ( LoadLibrary failed) or the provider's WSPStartup or NSPStartup function failed.
    #[error("service provider failed to initialize")]
    Providerfailedinit = 10106,

    /// System call failure.
    ///
    /// A system call that should never fail has failed. This is a generic error code, returned under various conditions. Returned when a system call that should never fail does fail. For example, if a call to WaitForMultipleEvents fails or one of the registry functions fails trying to manipulate the protocol/namespace catalogs. Returned when a provider does not return SUCCESS and does not provide an extended error code. Can indicate a service provider implementation error.
    #[error("system call failure")]
    Syscallfailure = 10107,

    /// Service not found.
    ///
    /// No such service is known. The service cannot be found in the specified name space.
    #[error("service not found")]
    ServiceNotFound = 10108,

    /// Class type not found.
    ///
    /// The specified class was not found.
    #[error("class type not found")]
    TypeNotFound = 10109,

    /// No more results.
    ///
    /// No more results can be returned by the WSALookupServiceNext function.
    #[error("no more results")]
    ENoMore = 10110,

    /// Call was canceled.
    ///
    /// A call to the WSALookupServiceEnd function was made while this call was still processing. The call has been canceled.
    #[error("call was canceled")]
    ECancelled = 10111,

    /// Database query was refused.
    ///
    /// A database query failed because it was actively refused.
    #[error("database query was refused")]
    Refused = 10112,

    /// Host not found.
    ///
    /// No such host is known. The name is not an official host name or alias, or it cannot be found in the database(s) being queried. This error may also be returned for protocol and service queries, and means that the specified name could not be found in the relevant database.
    #[error("host not found")]
    HostNotFound = 11001,

    /// Nonauthoritative host not found.
    ///
    /// This is usually a temporary error during host name resolution and means that the local server did not receive a response from an authoritative server. A retry at some time later may be successful.
    #[error("nonauthoritative host not found")]
    TryAgain = 11002,

    /// This is a nonrecoverable error.
    ///
    /// This indicates that some sort of nonrecoverable error occurred during a database lookup. This may be because the database files (for example, BSD-compatible HOSTS, SERVICES, or PROTOCOLS files) could not be found, or a DNS request was returned by the server with a severe error.
    #[error("this is a nonrecoverable error")]
    NoRecovery = 11003,

    /// Valid name, no data record of requested type.
    ///
    /// The requested name is valid and was found in the database, but it does not have the correct associated data being resolved for. The usual example for this is a host name-to-address translation attempt (using gethostbyname or WSAAsyncGetHostByName ) which uses the DNS (Domain Name Server). An MX record is returned but no A record—indicating the host itself exists, but is not directly reachable.
    #[error("valid name, no data record of requested type")]
    NoData = 11004,

    /// QoS receivers.
    ///
    /// At least one QoS reserve has arrived.
    #[error("qos receivers")]
    QosReceivers = 11005,

    /// QoS senders.
    ///
    /// At least one QoS send path has arrived.
    #[error("qos senders")]
    QosSenders = 11006,

    /// No QoS senders.
    ///
    /// There are no QoS senders.
    #[error("no qos senders")]
    QosNoSenders = 11007,

    /// QoS no receivers.
    ///
    /// There are no QoS receivers.
    #[error("qos no receivers")]
    QosNoReceivers = 11008,

    /// QoS request confirmed.
    ///
    /// The QoS reserve request has been confirmed.
    #[error("qos request confirmed")]
    QosRequestConfirmed = 11009,

    /// QoS admission error.
    ///
    /// A QoS error occurred due to lack of resources.
    #[error("qos admission error")]
    QosAdmissionFailure = 11010,

    /// QoS policy failure.
    ///
    /// The QoS request was rejected because the policy system couldn't allocate the requested resource within the existing policy.
    #[error("qos policy failure")]
    QosPolicyFailure = 11011,

    /// QoS bad style.
    ///
    /// An unknown or conflicting QoS style was encountered.
    #[error("qos bad style")]
    QosBadStyle = 11012,

    /// QoS bad object.
    ///
    /// A problem was encountered with some part of the filterspec or the provider-specific buffer in general.
    #[error("qos bad object")]
    QosBadObject = 11013,

    /// QoS traffic control error.
    ///
    /// An error with the underlying traffic control (TC) API as the generic QoS request was converted for local enforcement by the TC API. This could be due to an out of memory error or to an internal QoS provider error.
    #[error("qos traffic control error")]
    QosTrafficCtrlError = 11014,

    /// QoS generic error.
    ///
    /// A general QoS error.
    #[error("qos generic error")]
    QosGenericError = 11015,

    /// QoS service type error.
    ///
    /// An invalid or unrecognized service type was found in the QoS flowspec.
    #[error("qos service type error")]
    QosEservicetype = 11016,

    /// QoS flowspec error.
    ///
    /// An invalid or inconsistent flowspec was found in the QOS structure.
    #[error("qos flowspec error")]
    QosEflowspec = 11017,

    /// Invalid QoS provider buffer.
    ///
    /// An invalid QoS provider-specific buffer.
    #[error("invalid qos provider buffer")]
    QosEprovspecbuf = 11018,

    /// Invalid QoS filter style.
    ///
    /// An invalid QoS filter style was used.
    #[error("invalid qos filter style")]
    QosEfilterstyle = 11019,

    /// Invalid QoS filter type.
    ///
    /// An invalid QoS filter type was used.
    #[error("invalid qos filter type")]
    QosEfiltertype = 11020,

    /// Incorrect QoS filter count.
    ///
    /// An incorrect number of QoS FILTERSPECs were specified in the FLOWDESCRIPTOR.
    #[error("incorrect qos filter count")]
    QosEfiltercount = 11021,

    /// Invalid QoS object length.
    ///
    /// An object with an invalid ObjectLength field was specified in the QoS provider-specific buffer.
    #[error("invalid qos object length")]
    QosEobjlength = 11022,

    /// Incorrect QoS flow count.
    ///
    /// An incorrect number of flow descriptors was specified in the QoS structure.
    #[error("incorrect qos flow count")]
    QosEflowcount = 11023,

    /// Unrecognized QoS object.
    ///
    /// An unrecognized object was found in the QoS provider-specific buffer.
    #[error("unrecognized qos object")]
    QosEunkownpsobj = 11024,

    /// Invalid QoS policy object.
    ///
    /// An invalid policy object was found in the QoS provider-specific buffer.
    #[error("invalid qos policy object")]
    QosEpolicyobj = 11025,

    /// Invalid QoS flow descriptor.
    ///
    /// An invalid QoS flow descriptor was found in the flow descriptor list.
    #[error("invalid qos flow descriptor")]
    QosEflowdesc = 11026,

    /// Invalid QoS provider-specific flowspec.
    ///
    /// An invalid or inconsistent flowspec was found in the QoS provider-specific buffer.
    #[error("invalid qos provider-specific flowspec")]
    QosEpsflowspec = 11027,

    /// Invalid QoS provider-specific filterspec.
    ///
    /// An invalid FILTERSPEC was found in the QoS provider-specific buffer.
    #[error("invalid qos provider-specific filterspec")]
    QosEpsfilterspec = 11028,

    /// Invalid QoS shape discard mode object.
    ///
    /// An invalid shape discard mode object was found in the QoS provider-specific buffer.
    #[error("invalid qos shape discard mode object")]
    QosEsdmodeobj = 11029,

    /// Invalid QoS shaping rate object.
    ///
    /// An invalid shaping rate object was found in the QoS provider-specific buffer.
    #[error("invalid qos shaping rate object")]
    QosEshaperateobj = 11030,

    /// Reserved policy QoS element type.
    ///
    /// A reserved policy element was found in the QoS provider-specific buffer.
    #[error("reserved policy qos element type")]
    QosReservedPetype = 11031,
}

impl TryFrom<WSA_ERROR> for WinsockError {
    type Error = i32;

    fn try_from(value: WSA_ERROR) -> Result<Self, Self::Error> {
        match value.0 {
            6 => Ok(Self::InvalidHandle),
            8 => Ok(Self::NotEnoughMemory),
            87 => Ok(Self::InvalidParameter),
            995 => Ok(Self::OperationAborted),
            996 => Ok(Self::IoIncomplete),
            997 => Ok(Self::IoPending),
            10004 => Ok(Self::Intr),
            10009 => Ok(Self::Badf),
            10013 => Ok(Self::Acces),
            10014 => Ok(Self::Fault),
            10022 => Ok(Self::Inval),
            10024 => Ok(Self::Mfile),
            10035 => Ok(Self::Wouldblock),
            10036 => Ok(Self::Inprogress),
            10037 => Ok(Self::Already),
            10038 => Ok(Self::Notsock),
            10039 => Ok(Self::Destaddrreq),
            10040 => Ok(Self::Msgsize),
            10041 => Ok(Self::Prototype),
            10042 => Ok(Self::Noprotoopt),
            10043 => Ok(Self::Protonosupport),
            10044 => Ok(Self::Socktnosupport),
            10045 => Ok(Self::Opnotsupp),
            10046 => Ok(Self::Pfnosupport),
            10047 => Ok(Self::Afnosupport),
            10048 => Ok(Self::Addrinuse),
            10049 => Ok(Self::Addrnotavail),
            10050 => Ok(Self::Netdown),
            10051 => Ok(Self::Netunreach),
            10052 => Ok(Self::Netreset),
            10053 => Ok(Self::Connaborted),
            10054 => Ok(Self::Connreset),
            10055 => Ok(Self::Nobufs),
            10056 => Ok(Self::Isconn),
            10057 => Ok(Self::Notconn),
            10058 => Ok(Self::Shutdown),
            10059 => Ok(Self::Toomanyrefs),
            10060 => Ok(Self::Timedout),
            10061 => Ok(Self::Connrefused),
            10062 => Ok(Self::Loop),
            10063 => Ok(Self::Nametoolong),
            10064 => Ok(Self::Hostdown),
            10065 => Ok(Self::Hostunreach),
            10066 => Ok(Self::Notempty),
            10067 => Ok(Self::Proclim),
            10068 => Ok(Self::Users),
            10069 => Ok(Self::Dquot),
            10070 => Ok(Self::Stale),
            10071 => Ok(Self::Remote),
            10091 => Ok(Self::Sysnotready),
            10092 => Ok(Self::Vernotsupported),
            10093 => Ok(Self::Notinitialised),
            10101 => Ok(Self::Discon),
            10102 => Ok(Self::Nomore),
            10103 => Ok(Self::Cancelled),
            10104 => Ok(Self::Invalidproctable),
            10105 => Ok(Self::Invalidprovider),
            10106 => Ok(Self::Providerfailedinit),
            10107 => Ok(Self::Syscallfailure),
            10108 => Ok(Self::ServiceNotFound),
            10109 => Ok(Self::TypeNotFound),
            10110 => Ok(Self::ENoMore),
            10111 => Ok(Self::ECancelled),
            10112 => Ok(Self::Refused),
            11001 => Ok(Self::HostNotFound),
            11002 => Ok(Self::TryAgain),
            11003 => Ok(Self::NoRecovery),
            11004 => Ok(Self::NoData),
            11005 => Ok(Self::QosReceivers),
            11006 => Ok(Self::QosSenders),
            11007 => Ok(Self::QosNoSenders),
            11008 => Ok(Self::QosNoReceivers),
            11009 => Ok(Self::QosRequestConfirmed),
            11010 => Ok(Self::QosAdmissionFailure),
            11011 => Ok(Self::QosPolicyFailure),
            11012 => Ok(Self::QosBadStyle),
            11013 => Ok(Self::QosBadObject),
            11014 => Ok(Self::QosTrafficCtrlError),
            11015 => Ok(Self::QosGenericError),
            11016 => Ok(Self::QosEservicetype),
            11017 => Ok(Self::QosEflowspec),
            11018 => Ok(Self::QosEprovspecbuf),
            11019 => Ok(Self::QosEfilterstyle),
            11020 => Ok(Self::QosEfiltertype),
            11021 => Ok(Self::QosEfiltercount),
            11022 => Ok(Self::QosEobjlength),
            11023 => Ok(Self::QosEflowcount),
            11024 => Ok(Self::QosEunkownpsobj),
            11025 => Ok(Self::QosEpolicyobj),
            11026 => Ok(Self::QosEflowdesc),
            11027 => Ok(Self::QosEpsflowspec),
            11028 => Ok(Self::QosEpsfilterspec),
            11029 => Ok(Self::QosEsdmodeobj),
            11030 => Ok(Self::QosEshaperateobj),
            11031 => Ok(Self::QosReservedPetype),
            other => Err(other),
        }
    }
}
