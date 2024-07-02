#include <unistd.h>
#include "linuxinotify.h"

void c_inotify_init(const int flags, int result[2]) {
  /*
  Initialize a new INotify Instance.
  Args:
    flags: The Flags to use when initializing the INotify Instance.
    result: A "tuple" of (errno, fd).
  */
  errno = 0; // Reset errno
  result[1] = inotify_init1(flags); // Set the File Descriptor
  if (errno == 0) { result[0] = INE_NONE; }
  else if (errno == EINVAL) { result[0] = (int)INE_BAD_ARGS; }
  else if (errno == EMFILE || errno == ENFILE) { result[0] = (int)INE_LIMIT; }
  else if (errno == ENOMEM) { result[0] = (int)INE_NO_MEM; }
  else { result[0] = (int)INE_UNDEFINED; }
  return;
}

void c_inotify_add_watch(const int fd, const char *path, const int mask, int result[2]) {
  /*
  Add a Watch to a Path.
  Args:
    fd: The File Descriptor of the INotify Instance.
    path: The Path to watch.
    mask: The Mask of Events to watch for.
    result: A "tuple" of (errno, watchdesc).
  */
  errno = 0; // Reset errno
  result[1] = inotify_add_watch(fd, path, mask);
  if (errno == 0) { result[0] = INE_NONE; }
  else if (errno == EINVAL || errno == EFAULT || errno == EBADF) { result[0] = (int)INE_BAD_ARGS; }
  else if (errno == EACCES) { result[0] = (int)INE_NO_READ; }
  else if (errno == EEXIST) { result[0] = (int)INE_ALREADY_EXIST; }
  else if (errno == ENOENT || errno == ENAMETOOLONG || errno == ENOTDIR ) { result[0] = (int)INE_BAD_PATH; }
  else if (errno == ENOSPC ) { result[0] = (int)INE_LIMIT; }
  else { result[0] = (int)INE_UNDEFINED; }
  return;
}

void c_inotify_rm_watch(const int fd, const int watchdesc, int result[1]) {
  /*
  Remove a Watch from a Path.
  Args:
    fd: The File Descriptor of the INotify Instance.
    watchdesc: The Watch Descriptor to remove.
    result: A "tuple" of (errno,).
  */
  errno = 0; // Reset errno
  int res = inotify_rm_watch(fd, watchdesc);
  if (res == 0) { result[0] = INE_NONE; }
  else if (errno == EBADF || errno == EINVAL) { result[0] = (int)INE_BAD_ARGS; }
  else { result[0] = (int)INE_UNDEFINED; }
  return;
}

void c_read_event(const int fd, struct inotify_event *event, int result[1]) {
  /*
  Read an Event from the INotify Instance.
  Args:
    fd: The File Descriptor of the INotify Instance.
    result: A "tuple" of (errno, event) where event 
  */
  errno = 0; // Reset errno
  // Read Length: sizeof(struct inotify_event) + NAME_MAX + 1
  ssize_t len = read(fd, event, INOTEV_READ_SIZE);
  if (len == -1) {
    if (errno == EAGAIN || errno == EWOULDBLOCK) { result[0] = INE_BLOCK; }
    else if (errno == EBADF || errno == EFAULT || errno == EINVAL) { result[0] = (int)INE_BAD_ARGS; }
    else { result[0] = (int)INE_UNDEFINED; }
  } else { result[0] = INE_NONE; }
  return;
}