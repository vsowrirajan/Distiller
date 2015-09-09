package com.mapr.distiller.cli.base.common;

public class Errno {
  public static final int SUCCESS = 0;
  public static final int EPERM = 1;
  public static final int ENOENT = 2;
  public static final int ESRCH = 3;
  public static final int EINTR = 4;
  public static final int EIO = 5;
  public static final int ENXIO = 6;
  public static final int E2BIG = 7;
  public static final int ENOEXEC = 8;
  public static final int EBADF = 9;
  public static final int ECHILD = 10;
  public static final int EAGAIN = 11;
  public static final int ENOMEM = 12;
  public static final int EACCES = 13;
  public static final int EFAULT = 14;
  public static final int ENOTBLK = 15;
  public static final int EBUSY = 16;
  public static final int EEXIST = 17;
  public static final int EXDEV = 18;
  public static final int ENODEV = 19;
  public static final int ENOTDIR = 20;
  public static final int EISDIR = 21;
  public static final int EINVAL = 22;
  public static final int ENFILE = 23;
  public static final int EMFILE = 24;
  public static final int ENOTTY = 25;
  public static final int ETXTBSY = 26;
  public static final int EFBIG = 27;
  public static final int ENOSPC = 28;
  public static final int ESPIPE = 29;
  public static final int EROFS = 30;
  public static final int EMLINK = 31;
  public static final int EPIPE = 32;
  public static final int EDOM = 33;
  public static final int ERANGE = 34;
  public static final int EDEADLK = 35;
  public static final int ENAMETOOLONG = 36;
  public static final int ENOLCK = 37;
  public static final int ENOSYS = 38;
  public static final int ENOTEMPTY = 39;
  public static final int ELOOP = 40;
  public static final int EWOULDBLOCK = EAGAIN;
  public static final int ENOMSG = 42;
  public static final int EIDRM = 43;
  public static final int ECHRNG = 44;
  public static final int EL2NSYNC = 45;
  public static final int EL3HLT = 46;
  public static final int EL3RST = 47;
  public static final int ELNRNG = 48;
  public static final int EUNATCH = 49;
  public static final int ENOCSI = 50;
  public static final int EL2HLT = 51;
  public static final int EBADE = 52;
  public static final int EBADR = 53;
  public static final int EXFULL = 54;
  public static final int ENOANO = 55;
  public static final int EBADRQC = 56;
  public static final int EBADSLT = 57;
  public static final int EDEADLOCK = EDEADLK;
  public static final int EBFONT = 59;
  public static final int ENOSTR = 60;
  public static final int ENODATA = 61;
  public static final int ETIME = 62;
  public static final int ENOSR = 63;
  public static final int ENONET = 64;
  public static final int ENOPKG = 65;
  public static final int EREMOTE = 66;
  public static final int ENOLINK = 67;
  public static final int EADV = 68;
  public static final int ESRMNT = 69;
  public static final int ECOMM = 70;
  public static final int EPROTO = 71;
  public static final int EMULTIHOP = 72;
  public static final int EDOTDOT = 73;
  public static final int EBADMSG = 74;
  public static final int EOVERFLOW = 75;
  public static final int ENOTUNIQ = 76;
  public static final int EBADFD = 77;
  public static final int EREMCHG = 78;
  public static final int ELIBACC = 79;
  public static final int ELIBBAD = 80;
  public static final int ELIBSCN = 81;
  public static final int ELIBMAX = 82;
  public static final int ELIBEXEC = 83;
  public static final int EILSEQ = 84;
  public static final int ERESTART = 85;
  public static final int ESTRPIPE = 86;
  public static final int EUSERS = 87;
  public static final int ENOTSOCK = 88;
  public static final int EDESTADDRREQ = 89;
  public static final int EMSGSIZE = 90;
  public static final int EPROTOTYPE = 91;
  public static final int ENOPROTOOPT = 92;
  public static final int EPROTONOSUPPORT = 93;
  public static final int ESOCKTNOSUPPORT = 94;
  public static final int EOPNOTSUPP = 95;
  public static final int EPFNOSUPPORT = 96;
  public static final int EAFNOSUPPORT = 97;
  public static final int EADDRINUSE = 98;
  public static final int EADDRNOTAVAIL = 99;
  public static final int ENETDOWN = 100;
  public static final int ENETUNREACH = 101;
  public static final int ENETRESET = 102;
  public static final int ECONNABORTED = 103;
  public static final int ECONNRESET = 104;
  public static final int ENOBUFS = 105;
  public static final int EISCONN = 106;
  public static final int ENOTCONN = 107;
  public static final int ESHUTDOWN = 108;
  public static final int ETOOMANYREFS = 109;
  public static final int ETIMEDOUT = 110;
  public static final int ECONNREFUSED = 111;
  public static final int EHOSTDOWN = 112;
  public static final int EHOSTUNREACH = 113;
  public static final int EALREADY = 114;
  public static final int EINPROGRESS = 115;
  public static final int ESTALE = 116;
  public static final int EUCLEAN = 117;
  public static final int ENOTNAM = 118;
  public static final int ENAVAIL = 119;
  public static final int EISNAM = 120;
  public static final int EREMOTEIO = 121;
  public static final int EDQUOT = 122;
  public static final int ENOMEDIUM = 123;
  public static final int EMEDIUMTYPE = 124;
  public static final int ECANCELED = 125;
  public static final int ENOKEY = 126;
  public static final int EKEYEXPIRED = 127;
  public static final int EKEYREVOKED = 128;
  public static final int EKEYREJECTED = 129;
  public static final int EOWNERDEAD = 130;
  public static final int ENOTRECOVERABLE = 131;
  public static final int ERFKILL = 132;
  public static final int EUCLUSTER = 133;
  public static final int EAUTH = 134;
  public static final int ENOUSER = 135;
  public static final int EDIFFCLUSTER = 136;

  public static final int MAX_ERRNO = 137;  // sentinel

  final static String [] errnoStrings = new String[] {
    "Success", // 0
    "Operation not permitted",
    "No such file or directory",
    "No such process",
    "Interrupted system call",
    "Input/output error", // 5
    "No such device or address",
    "Argument list too long",
    "Exec format error",
    "Bad file descriptor",
    "No child processes", // 10
    "Resource temporarily unavailable",
    "Cannot allocate memory",
    "Permission denied",
    "Bad address",
    "Block device required", // 15
    "Device or resource busy",
    "File exists",
    "Invalid cross-device link",
    "No such device",
    "Not a directory", // 20
    "Is a directory",
    "Invalid argument",
    "Too many open files in system",
    "Too many open files",
    "Inappropriate ioctl for device", // 25
    "Text file busy",
    "File too large",
    "No space left on device",
    "Illegal seek",
    "Read-only file system", // 30
    "Too many links",
    "Broken pipe",
    "Numerical argument out of domain",
    "Numerical result out of range",
    "Resource deadlock avoided", // 35
    "File name too long",
    "No locks available",
    "Function not implemented",
    "Directory not empty",
    "Too many levels of symbolic links", // 40
    "Unknown error 41",
    "No message of desired type",
    "Identifier removed",
    "Channel number out of range",
    "Level 2 not synchronized", // 45
    "Level 3 halted",
    "Level 3 reset",
    "Link number out of range",
    "Protocol driver not attached",
    "No CSI structure available", // 50
    "Level 2 halted",
    "Invalid exchange",
    "Invalid request descriptor",
    "Exchange full",
    "No anode", // 55
    "Invalid request code",
    "Invalid slot",
    "Unknown error 58",
    "Bad font file format",
    "Device not a stream", // 60
    "No data available",
    "Timer expired",
    "Out of streams resources",
    "Machine is not on the network",
    "Package not installed", // 65
    "Object is remote",
    "Link has been severed",
    "Advertise error",
    "Srmount error",
    "Communication error on send", // 70
    "Protocol error",
    "Multihop attempted",
    "RFS specific error",
    "Bad message",
    "Value too large for defined data type", // 75
    "Name not unique on network",
    "File descriptor in bad state",
    "Remote address changed",
    "Can not access a needed shared library",
    "Accessing a corrupted shared library", // 80
    ".lib section in a.out corrupted",
    "Attempting to link in too many shared libraries",
    "Cannot exec a shared library directly",
    "Invalid or incomplete multibyte or wide character",
    "Interrupted system call should be restarted", // 85
    "Streams pipe error",
    "Too many users",
    "Socket operation on non-socket",
    "Destination address required",
    "Message too long", // 90
    "Protocol wrong type for socket",
    "Protocol not available",
    "Protocol not supported",
    "Socket type not supported",
    "Operation not supported", // 95
    "Protocol family not supported",
    "Address family not supported by protocol",
    "Address already in use",
    "Cannot assign requested address",
    "Network is down", // 100
    "Network is unreachable",
    "Network dropped connection on reset",
    "Software caused connection abort",
    "Connection reset by peer",
    "No buffer space available", // 105
    "Transport endpoint is already connected",
    "Transport endpoint is not connected",
    "Cannot send after transport endpoint shutdown",
    "Too many references: cannot splice",
    "Connection timed out", // 110
    "Connection refused",
    "Host is down",
    "No route to host",
    "Operation already in progress",
    "Operation now in progress", // 115
    "Stale NFS file handle",
    "Structure needs cleaning",
    "Not a XENIX named type file",
    "No XENIX semaphores available",
    "Is a named type file", // 120
    "Remote I/O error",
    "Disk quota exceeded",
    "No medium found",
    "Wrong medium type",
    "Operation canceled", // 125
    "Required key not available",
    "Key has expired",
    "Key has been revoked",
    "Key was rejected by service",
    "Owner died", // 130
    "State not recoverable",
    "Unknown error 132",
    "Unknown cluster parameter provided",
    "Authentication Failure",
    "User or group not found", // 135
    "Path has symlinks pointing to another cluster",
  };

  // Security error
  public static final int E_NOT_ENOUGH_PRIVILEGES = EPERM;

  // Common errors shared with FileServer
  public static final int ErrNotMasterForContainer = ESRCH;
  public static final int ErrContainerNotOnNode = ENODEV;
  public static final int ErrContainerInGfsck = EBUSY;
  public static final int ErrServerRetry = ENAVAIL;

  // CLDB Errors
  public static final int EOPFORBIDDEN = 1000;
  public static final int MAX_CLDB_ERROR = 10000;
  
  // CLI Errors
  public static final int EMISSING = 10001;
  public static final int EINVALMISC = 10002;
  public static final int EOPFAILED = 10003;
  public static final int INTERROR = 10004;
  public static final int ENOTEXIST = 10005;
  public static final int EZKCANTCONNECT = 10006;
  public static final int ECLDBINFOINVALID = 10007;
  public static final int ENOMATCH = 10008;
  public static final int ERPCFAILED = 10009;
  public static final int ENOTLICENSED = 10010;
  public static final int ENONOESINTOPOLOGY = 10011;
  public static final int ENODESEXCEEDMAX = 10012;
  public static final int EDIALHOMEDISABLED = 10013;
  public static final int ENOMETRICSFORDAY = 10014;
  public static final int EOPNOTPERMITTEDINTERNALVOL = 10015;
  public static final int EJM = 10016; // generic Job management error
  public static final int EJMQUERYFAILURE = 10017;
  public static final int EJMFETCHJTURL = 10018;

  // offset from MAX_CLDB_ERROR
  final static String[] maprErrnoStrings = {
      "", // MAX_CLDB_ERROR
      "EMISSING",
      "EINVALMISC",
      "EOPFAILED",
      "INTERROR",
      "ENOTEXIST",
      "EZKCANTCONNECT",
      "ECLDBINFOINVALID",
      "ENOMATCH",
      "ERPCFAILED",
      "No license for requested operation",
      "No nodes available in topology",
      "Number of nodes exceed license",
      "Dialhome is disabled",
      "No metrics found for the given day",
      "Operation not permitted for internal volume",
      "Job manager error",
      "Failed to query the jobs database",
      "Failed to fetch the job tracker url"
  };

  public static String toString( int err) {
    if (err < MAX_ERRNO) {
      return errnoStrings[err];
    } else if (err >= EMISSING && err <= EJMFETCHJTURL) {
      return maprErrnoStrings[err-MAX_CLDB_ERROR];
    }
    return "No Error Description found for errCode: " + err;
  }
}
