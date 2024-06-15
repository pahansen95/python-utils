void c_read_from_fd_into_buffer(
  int fd,
  char *buffer,
  size_t count,
  size_t buf_offset,
  int result[2]
);
void c_write_from_buffer_into_fd(
  int fd,
  char *buffer,
  size_t count,
  size_t buf_offset,
  int result[2]
);
