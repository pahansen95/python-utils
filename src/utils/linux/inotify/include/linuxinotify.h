// INotify Flags
// IN_ACCESS
// IN_ATTRIB
// IN_CLOSE_WRITE
// IN_CLOSE_NOWRITE
// IN_CREATE
// IN_DELETE
// IN_DELETE_SELF
// IN_MODIFY
// IN_MOVE_SELF
// IN_MOVED_FROM
// IN_MOVED_TO
// IN_OPEN
// IN_MOVED
// IN_CLOSE
// IN_DONT_FOLLOW
// IN_EXCL_UNLINK
// IN_MASK_ADD
// IN_ONESHOT
// IN_ONLYDIR
// IN_MASK_CREATE
// IN_IGNORED
// IN_ISDIR
// IN_Q_OVERFLOW
// IN_UNMOUNT

// Enums
typedef enum {
  INE_NONE,
  INE_UNDEFINED,

  /* Common Errors */
  INE_BAD_ARGS, // A Bad Value was passed
  INE_NO_MEM, // Out of Memory
  INE_LIMIT, // Some File or Watch Limit was reached

  /* Add Watch/Remove Errors */
  INE_NO_READ, // The File Descriptor does not have read permissions
  INE_ALREADY_EXIST, // The Watch for the Path already exists
  INE_BAD_PATH, // The Path is invalid for some reason
} c_INotifyError;

// INotify Interface
void c_inotify_init(int flags, int result[2]);
void c_inotify_add_watch(int fd, const char *path, int mask, int result[2]);
void c_inotify_rm_watch(int fd, int watchdesc, int result[1]);
