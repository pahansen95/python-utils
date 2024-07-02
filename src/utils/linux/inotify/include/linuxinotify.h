#ifndef _LINUXINOTIFY_H
#define _LINUXINOTIFY_H

#include <linux/limits.h> // Include the limits header
#include <sys/inotify.h> // Include the inotify header
#include <errno.h>

static const int INOTEV_NAME_SIZE = NAME_MAX + 1;
static const int INOTEV_READ_SIZE = sizeof(struct inotify_event) + NAME_MAX + 1;

// Enums for errors
typedef enum {
  INE_NONE, // No Error Occured
  INE_UNDEFINED, // An Undefined Error Occurred
  INE_BLOCK, // The Operation would block

  /* Common Errors */
  INE_BAD_ARGS, // A Bad Value was passed
  INE_NO_MEM, // Out of Memory
  INE_LIMIT, // Some File or Watch Limit was reached

  /* Add Watch/Remove Errors */
  INE_NO_READ, // The File Descriptor does not have read permissions
  INE_ALREADY_EXIST, // The Watch for the Path already exists
  INE_BAD_PATH, // The Path is invalid for some reason
} c_INotifyError;

/* Wraps the inotify `IN_*` Constants as an enum for use in Python*/
typedef enum {
  INC_ACCESS        = IN_ACCESS,
  INC_MODIFY        = IN_MODIFY,
  INC_ATTRIB        = IN_ATTRIB,
  INC_CLOSE_WRITE   = IN_CLOSE_WRITE,
  INC_CLOSE_NOWRITE = IN_CLOSE_NOWRITE,
  INC_OPEN          = IN_OPEN,
  INC_MOVED_FROM    = IN_MOVED_FROM,
  INC_MOVED_TO      = IN_MOVED_TO,
  INC_CREATE        = IN_CREATE,
  INC_DELETE        = IN_DELETE,
  INC_DELETE_SELF   = IN_DELETE_SELF,
  INC_MOVE_SELF     = IN_MOVE_SELF,
  INC_UNMOUNT       = IN_UNMOUNT,
  INC_Q_OVERFLOW    = IN_Q_OVERFLOW,
  INC_IGNORED       = IN_IGNORED,
  INC_CLOSE         = IN_CLOSE,
  INC_MOVE          = IN_MOVE,
  INC_ONLYDIR       = IN_ONLYDIR,
  INC_DONT_FOLLOW   = IN_DONT_FOLLOW,
  INC_EXCL_UNLINK   = IN_EXCL_UNLINK,
  INC_MASK_CREATE   = IN_MASK_CREATE,
  INC_MASK_ADD      = IN_MASK_ADD,
  INC_ISDIR         = IN_ISDIR,
  INC_ONESHOT       = IN_ONESHOT,
  INC_ALL_EVENTS    = IN_ALL_EVENTS,
  INC_NONBLOCK      = IN_NONBLOCK,
  INC_CLOEXEC       = IN_CLOEXEC,
} c_INotifyConst;

// INotify Interface
void c_inotify_init(const int flags, int result[2]);
void c_inotify_add_watch(const int fd, const char *path, const int mask, int result[2]);
void c_inotify_rm_watch(const int fd, const int watchdesc, int result[1]);
void c_read_event(const int fd, struct inotify_event *event, int result[1]);

#endif /* _LINUXINOTIFY_H */