#include <unistd.h>
#include <errno.h>

static enum IOErrorReason {
  IOE_NONE = 0,
  IOE_BLOCK = 1,
  IOE_EOF = 2,
  IOE_CLOSED = 3,
  IOE_IOERR = 4,
  IOE_INTERRUPT = 5,
  IOE_UNHANDLED = 127,
};

void c_read_from_fd_into_buffer(
  int fd,
  char *buffer,
  size_t count,
  size_t buf_offset,
  int result[2]
) {
  /*
  Read from a File Descriptor into a Buffer returning any errors that occur & the bytes read.
  Args:
    fd: The File Descriptor to read from.
    buffer: The Buffer to read into; a pointer to a byte array.
    count: The number of bytes to read.
    buf_offset: The offset index at which to start writing into the Buffer.
    result: A "tuple" of (IOErrorReason, bytes_read).
  */

  char *buffer_offset = buffer + buf_offset;
  errno = 0; // Reset errno
  ssize_t n = read(fd, buffer_offset, count);

  if (n >= 0) {
    // Successful read
    if (n == 0) { result[0] = (int)IOE_EOF; }
    else { result[0] = (int)IOE_NONE; }
    result[1] = (int)n;
  } else {
    // An error occurred
    if (errno == EAGAIN || errno == EWOULDBLOCK) { result[0] = (int)IOE_BLOCK; }
    else if (errno == EBADF) { result[0] = (int)IOE_CLOSED; }
    else if (errno == EINTR) { result[0] = (int)IOE_INTERRUPT; }
    else if (errno == EIO) { result[0] = (int)IOE_IOERR; }
    else { result[0] = (int)IOE_UNHANDLED + errno; }
    result[1] = 0;
  }
}

void c_write_from_buffer_into_fd(
  int fd,
  char *buffer,
  size_t count,
  size_t buf_offset,
  int result[2]
) {
  /*
  Write to a File Descriptor from a Buffer returning any errors that occur & the bytes written.
  Args:
    fd: The File Descriptor to write to.
    buffer: The Buffer to write from; a pointer to a byte array.
    count: The number of bytes to write.
    buf_offset: The offset index at which to start reading from the Buffer.
    result: A "tuple" of (IOErrorReason, bytes_written).
  */

  char *buffer_offset = buffer + buf_offset;
  errno = 0; // Reset errno
  ssize_t n = write(fd, buffer_offset, count);

  if (n >= 0) {
    // Successful write
    result[0] = (int)IOE_NONE;
    result[1] = (int)n;
  } else {
    // An error occurred
    if (errno == EAGAIN || errno == EWOULDBLOCK) { result[0] = (int)IOE_BLOCK; }
    else if (errno == EBADF) { result[0] = (int)IOE_CLOSED; }
    else if (errno == EINTR) { result[0] = (int)IOE_INTERRUPT; }
    else if (errno == EIO) { result[0] = (int)IOE_IOERR; }
    else { result[0] = (int)IOE_UNHANDLED + errno; }
    result[1] = 0;
  }
}
