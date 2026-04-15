/// Copyright 2026 Theo Lincke
/// 
/// Licensed under the Apache License, Version 2.0 (the "License");
/// you may not use this file except in compliance with the License.
/// You may obtain a copy of the License at
/// 
///     http://www.apache.org/licenses/LICENSE-2.0
/// 
/// Unless required by applicable law or agreed to in writing, software
/// distributed under the License is distributed on an "AS IS" BASIS,
/// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
/// See the License for the specific language governing permissions and
/// limitations under the License.

#include "tlclib/dev/assert.h"
#include "tlclib/dev/error.h"
#include "tlclib/intf/logging.h"

#include <dirent.h>
#include <errno.h>
#include <fcntl.h>
#include <limits.h>
#include <pthread.h>
#include <stdarg.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/stat.h>
#include <sys/uio.h>
#include <unistd.h>

// os

#ifndef NDEBUG
static bool
fd_is_open (const int fd)
{
  return fcntl (fd, F_GETFD) != -1 || errno != EBADF;
}
#endif

DEFINE_DBG_ASSERT (i_file, i_file, fp, {
  ASSERT (fp);
  ASSERT (fd_is_open (fp->fd));
})

////////////////////////////////////////////////////////////
// OPEN / CLOSE
err_t
i_open_rw (i_file *dest, const char *fname, error *e)
{
  const int fd = open (fname, O_RDWR | O_CREAT, 0644);

  if (fd == -1)
    {
      error_causef (e, ERR_IO, "open_rw %s: %s", fname, strerror (errno));
      return error_trace (e);
    }
  *dest = (i_file){ .fd = fd };

  DBG_ASSERT (i_file, dest);

  return SUCCESS;
}

err_t
i_open_r (i_file *dest, const char *fname, error *e)
{
  const int fd = open (fname, O_RDONLY | O_CREAT, 0644);

  if (fd == -1)
    {
      error_causef (e, ERR_IO, "open_r %s: %s", fname, strerror (errno));
      return error_trace (e);
    }
  *dest = (i_file){ .fd = fd };

  DBG_ASSERT (i_file, dest);

  return SUCCESS;
}

err_t
i_open_w (i_file *dest, const char *fname, error *e)
{
  const int fd = open (fname, O_WRONLY | O_CREAT, 0644);

  if (fd == -1)
    {
      error_causef (e, ERR_IO, "open_w %s: %s", fname, strerror (errno));
      return error_trace (e);
    }
  *dest = (i_file){ .fd = fd };

  DBG_ASSERT (i_file, dest);

  return SUCCESS;
}

err_t
i_close (const i_file *fp, error *e)
{
  DBG_ASSERT (i_file, fp);
  const int ret = close (fp->fd);
  if (ret)
    {
      return error_causef (e, ERR_IO, "close: %s", strerror (errno));
    }
  return SUCCESS;
}

err_t
i_fsync (const i_file *fp, error *e)
{
  DBG_ASSERT (i_file, fp);
  const int ret = fsync (fp->fd);
  if (ret)
    {
      return error_causef (e, ERR_IO, "fsync: %s", strerror (errno));
    }
  return SUCCESS;
}

////////////////////////////////////////////////////////////
// Positional Read / Write */

i64
i_pread_some (const i_file *fp, void *dest, const u64 n, const u64 offset,
              error *e)
{
  DBG_ASSERT (i_file, fp);
  ASSERT (dest);
  ASSERT (n > 0);

  const ssize_t ret = pread (fp->fd, dest, n, (size_t)offset);

  if (ret < 0 && errno != EINTR)
    {
      return error_causef (e, ERR_IO, "pread: %s", strerror (errno));
    }

  return (i64)ret;
}

i64
i_pread_all (const i_file *fp, void *dest, const u64 n, const u64 offset,
             error *e)
{
  DBG_ASSERT (i_file, fp);
  ASSERT (dest);
  ASSERT (n > 0);

  u8 *_dest = (u8 *)dest;
  u64 nread = 0;

  while (nread < n)
    {
      // Do read
      ASSERT (n > nread);
      const ssize_t _nread = pread (fp->fd, _dest + nread, n - nread, offset + nread);

      // EOF
      if (_nread == 0)
        {
          return (i64)nread;
        }

      // Error
      if (_nread < 0 && errno != EINTR)
        {
          return error_causef (e, ERR_IO, "pread: %s", strerror (errno));
        }

      nread += (i64)_nread;
    }

  ASSERT (nread == n);

  return nread;
}

err_t
i_pread_all_expect (i_file *fp, void *dest, const u64 n, const u64 offset,
                    error *e)
{
  const i64 ret = i_pread_all (fp, dest, n, offset, e);
  WRAP (ret);

  if ((u64)ret != n)
    {
      return error_causef (e, ERR_CORRUPT,
                           "pread: short read (got %" PRId64 " of %" PRId64
                           " bytes)",
                           ret, (i64)n);
    }

  return SUCCESS;
}

i64
i_pwrite_some (const i_file *fp, const void *src, const u64 n,
               const u64 offset, error *e)
{
  DBG_ASSERT (i_file, fp);
  ASSERT (src);
  ASSERT (n > 0);

  const ssize_t ret = pwrite (fp->fd, src, n, (size_t)offset);
  if (ret < 0 && errno != EINTR)
    {
      return error_causef (e, ERR_IO, "pwrite: %s", strerror (errno));
    }
  return (i64)ret;
}

err_t
i_pwrite_all (const i_file *fp, const void *src, const u64 n,
              const u64 offset, error *e)
{
  DBG_ASSERT (i_file, fp);
  ASSERT (src);
  ASSERT (n > 0);

  const u8 *_src = (u8 *)src;
  u64 nwrite = 0;

  while (nwrite < n)
    {
      // Do write
      ASSERT (n > nwrite);
      const ssize_t _nwrite = pwrite (fp->fd, _src + nwrite, n - nwrite, offset + nwrite);

      // Error
      if (_nwrite < 0 && errno != EINTR)
        {
          return error_causef (e, ERR_IO, "pwrite: %s", strerror (errno));
        }

      nwrite += _nwrite;
    }

  ASSERT (nwrite == n);

  return SUCCESS;
}

////////////////////////////////////////////////////////////
// IO Vec

i64
i_writev_some (const i_file *fp, const struct bytes *src, const int iovcnt,
               error *e)
{
  DBG_ASSERT (i_file, fp);
  ASSERT (src);
  ASSERT (iovcnt > 0);
  ASSERT (iovcnt <= 2);

  // Use stack for conversion
  struct iovec sys_iov[2];

  for (int i = 0; i < iovcnt; i++)
    {
      sys_iov[i].iov_base = src[i].head;
      sys_iov[i].iov_len = src[i].len;
    }

  const ssize_t ret = writev (fp->fd, sys_iov, iovcnt);
  if (ret < 0 && errno != EINTR)
    {
      return error_causef (e, ERR_IO, "writev: %s", strerror (errno));
    }
  return (i64)ret;
}

err_t
i_writev_all (const i_file *fp, struct bytes *iov, const int iovcnt,
              error *e)
{
  DBG_ASSERT (i_file, fp);
  ASSERT (iov);
  ASSERT (iovcnt > 0);
  ASSERT (iovcnt <= 2);

  u64 total = 0;
  for (int i = 0; i < iovcnt; i++)
    {
      total += iov[i].len;
    }
  ASSERT (total > 0);

  u64 nwritten = 0;
  struct bytes *cur_iov = iov;
  int cur_iovcnt = iovcnt;

  while (nwritten < total)
    {
      struct iovec sys_iov[2];
      for (int i = 0; i < cur_iovcnt; i++)
        {
          sys_iov[i].iov_base = cur_iov[i].head;
          sys_iov[i].iov_len = cur_iov[i].len;
        }

      const ssize_t ret = writev (fp->fd, sys_iov, cur_iovcnt);

      if (ret < 0 && errno != EINTR)
        {
          return error_causef (e, ERR_IO, "writev: %s", strerror (errno));
        }

      if (ret <= 0)
        {
          continue;
        }

      nwritten += ret;

      u64 bytes_to_skip = ret;
      while (bytes_to_skip > 0 && cur_iovcnt > 0)
        {
          if (bytes_to_skip >= cur_iov->len)
            {
              bytes_to_skip -= cur_iov->len;
              cur_iov++;
              cur_iovcnt--;
            }
          else
            {
              cur_iov->head = (u8 *)cur_iov->head + bytes_to_skip;
              cur_iov->len -= bytes_to_skip;
              bytes_to_skip = 0;
            }
        }
    }

  ASSERT (nwritten == total);
  return SUCCESS;
}

i64
i_readv_some (const i_file *fp, const struct bytes *iov, const int iovcnt,
              error *e)
{
  DBG_ASSERT (i_file, fp);
  ASSERT (iov);
  ASSERT (iovcnt > 0);
  ASSERT (iovcnt <= 2);

  struct iovec sys_iov[2];
  for (int i = 0; i < iovcnt; i++)
    {
      sys_iov[i].iov_base = iov[i].head;
      sys_iov[i].iov_len = iov[i].len;
    }

  const ssize_t ret = readv (fp->fd, sys_iov, iovcnt);
  if (ret < 0 && errno != EINTR)
    {
      return error_causef (e, ERR_IO, "readv: %s", strerror (errno));
    }
  return (i64)ret;
}

i64
i_readv_all (const i_file *fp, struct bytes *iov, const int iovcnt,
             error *e)
{
  DBG_ASSERT (i_file, fp);
  ASSERT (iov);
  ASSERT (iovcnt > 0);
  ASSERT (iovcnt <= 2);

  u64 total = 0;
  for (int i = 0; i < iovcnt; i++)
    {
      total += iov[i].len;
    }
  ASSERT (total > 0);

  u64 nread = 0;
  struct bytes *cur_iov = iov;
  int cur_iovcnt = iovcnt;

  while (nread < total)
    {
      struct iovec sys_iov[2];
      for (int i = 0; i < cur_iovcnt; i++)
        {
          sys_iov[i].iov_base = cur_iov[i].head;
          sys_iov[i].iov_len = cur_iov[i].len;
        }

      const ssize_t ret = readv (fp->fd, sys_iov, cur_iovcnt);

      if (ret < 0 && errno != EINTR)
        {
          return error_causef (e, ERR_IO, "readv: %s", strerror (errno));
        }

      if (ret == 0)
        {
          break;
        }

      if (ret < 0)
        {
          continue;
        }

      nread += ret;

      u64 bytes_to_skip = ret;
      while (bytes_to_skip > 0 && cur_iovcnt > 0)
        {
          if (bytes_to_skip >= cur_iov->len)
            {
              bytes_to_skip -= cur_iov->len;
              cur_iov++;
              cur_iovcnt--;
            }
          else
            {
              cur_iov->head = (u8 *)cur_iov->head + bytes_to_skip;
              cur_iov->len -= bytes_to_skip;
              bytes_to_skip = 0;
            }
        }
    }

  return (i64)nread;
}

////////////////////////////////////////////////////////////
// Stream Read / Write

i64
i_read_some (const i_file *fp, void *dest, const u64 nbytes, error *e)
{
  DBG_ASSERT (i_file, fp);
  ASSERT (dest);
  ASSERT (nbytes > 0);

  i_log_trace ("read %llu bytes\n", nbytes);
  const ssize_t ret = read (fp->fd, dest, nbytes);
  i_log_trace ("read returned %ld\n", ret);

  if (ret < 0)
    {
      if (errno == EINTR || errno == EWOULDBLOCK)
        {
          i_log_trace ("read: EINTR/EWOULDBLOCK, "
                       "returning 0\n");
          return 0;
        }
      return error_causef (e, ERR_IO, "read: %s", strerror (errno));
    }
  return (i64)ret;
}

i64
i_read_all (const i_file *fp, void *dest, const u64 nbytes, error *e)
{
  DBG_ASSERT (i_file, fp);
  ASSERT (dest);
  ASSERT (nbytes > 0);

  u8 *_dest = (u8 *)dest;
  u64 nread = 0;

  while (nread < nbytes)
    {
      // Do read
      ASSERT (nbytes > nread);
      const ssize_t _nread = read (fp->fd, _dest + nread, nbytes - nread);

      // EOF
      if (_nread == 0)
        {
          return (i64)nread;
        }

      // Error
      if (_nread < 0)
        {
          if (errno == EINTR || errno == EWOULDBLOCK)
            {
              return 0;
            }
          return error_causef (e, ERR_IO, "read: %s", strerror (errno));
        }

      nread += (i64)_nread;
    }

  ASSERT (nread == nbytes);

  return nread;
}

i64
i_read_all_expect (i_file *fp, void *dest, const u64 nbytes, error *e)
{
  const i64 ret = i_read_all (fp, dest, nbytes, e);
  WRAP (ret);

  if ((u64)ret != nbytes)
    {
      return error_causef (e, ERR_CORRUPT,
                           "read: short read (got %" PRId64 " of %" PRId64
                           " bytes)",
                           ret, (i64)nbytes);
    }

  return SUCCESS;
}

i64
i_write_some (const i_file *fp, const void *src, const u64 nbytes,
              error *e)
{
  DBG_ASSERT (i_file, fp);
  ASSERT (src);
  ASSERT (nbytes > 0);

  i_log_trace ("write %llu bytes\n", nbytes);
  const ssize_t ret = write (fp->fd, src, nbytes);
  i_log_trace ("write returned %ld\n", ret);
  if (ret < 0 && errno != EINTR)
    {
      return error_causef (e, ERR_IO, "write: %s", strerror (errno));
    }
  return (i64)ret;
}

err_t
i_write_all (const i_file *fp, const void *src, const u64 nbytes,
             error *e)
{
  DBG_ASSERT (i_file, fp);
  ASSERT (src);
  ASSERT (nbytes > 0);

  const u8 *_src = (u8 *)src;
  u64 nwrite = 0;

  while (nwrite < nbytes)
    {
      // Do read
      ASSERT (nbytes > nwrite);

      const ssize_t _nwrite = write (fp->fd, _src + nwrite, nbytes - nwrite);

      // Error
      if (_nwrite < 0 && errno != EINTR)
        {
          return error_causef (e, ERR_IO, "write: %s", strerror (errno));
        }

      nwrite += _nwrite;
    }

  ASSERT (nwrite == nbytes);

  return SUCCESS;
}

////////////////////////////////////////////////////////////
// OTHERS
err_t
i_truncate (const i_file *fp, const u64 bytes, error *e)
{
  if (ftruncate (fp->fd, bytes) == -1)
    {
      return error_causef (e, ERR_IO, "truncate: %s", strerror (errno));
    }

  return SUCCESS;
}

err_t
i_fallocate (i_file *fp, const u64 bytes, error *e)
{
#if defined(__APPLE__)
  fstore_t store = {
    .fst_flags = F_ALLOCATECONTIG,
    .fst_posmode = F_PEOFPOSMODE,
    .fst_offset = 0,
    .fst_length = (off_t)bytes,
  };

  if (fcntl (fp->fd, F_PREALLOCATE, &store) == -1)
    {
      store.fst_flags = F_ALLOCATEALL;
      if (fcntl (fp->fd, F_PREALLOCATE, &store) == -1)
        {
          return error_causef (e, ERR_IO, "F_PREALLOCATE: %s", strerror (errno));
        }
    }

  if (ftruncate (fp->fd, (off_t)bytes) == -1)
    {
      return error_causef (e, ERR_IO, "ftruncate: %s", strerror (errno));
    }
#else
  int ret = posix_fallocate (fp->fd, 0, (off_t)bytes);
  if (ret != 0)
    {
      return error_causef (e, ERR_IO, "posix_fallocate: %s", strerror (ret));
    }
#endif

  return SUCCESS;
}

i64
i_file_size (const i_file *fp, error *e)
{
  struct stat st;
  if (fstat (fp->fd, &st) == -1)
    {
      error_causef (e, ERR_IO, "fstat: %s", strerror (errno));
      return error_trace (e);
    }
  return (i64)st.st_size;
}

err_t
i_remove_quiet (const char *fname, error *e)
{
  const int ret = remove (fname);

  if (ret && errno != ENOENT)
    {
      error_causef (e, ERR_IO, "remove: %s", strerror (errno));
      return error_trace (e);
    }

  return SUCCESS;
}

err_t
i_mkstemp (i_file *dest, char *tmpl, error *e)
{
  const int fd = mkstemp (tmpl);
  if (fd == -1)
    {
      error_causef (e, ERR_IO, "mkstemp: %s", strerror (errno));
      return error_trace (e);
    }

  dest->fd = fd;
  return SUCCESS;
}

err_t
i_unlink (const char *name, error *e)
{
  if (unlink (name))
    {
      error_causef (e, ERR_IO, "unlink: %s", strerror (errno));
      return error_trace (e);
    }
  return SUCCESS;
}

err_t
i_mkdir (const char *name, error *e)
{
  if (mkdir (name, S_IRWXU | S_IRWXG | S_IRWXO))
    {
      error_causef (e, ERR_IO, "mkdir: %s", strerror (errno));
      return error_trace (e);
    }
  return SUCCESS;
}

err_t
i_mkdir_quiet (const char *name, error *e)
{
  if (mkdir (name, S_IRWXU | S_IRWXG | S_IRWXO) == 0)
    {
      return SUCCESS;
    }

  if (errno != EEXIST)
    {
      error_causef (e, ERR_IO, "mkdir: %s", strerror (errno));
      return error_trace (e);
    }

  // EEXIST — verify the existing path is actually a directory
  struct stat st;
  if (stat (name, &st) != 0)
    {
      error_causef (e, ERR_IO, "stat %s: %s", name, strerror (errno));
      return error_trace (e);
    }

  if (!S_ISDIR (st.st_mode))
    {
      error_causef (e, ERR_IO, "mkdir_quiet: %s exists but is not a directory",
                    name);
      return error_trace (e);
    }

  return SUCCESS;
}

err_t
i_rm_rf (const char *path, error *e)
{
  DIR *dir = opendir (path);
  if (dir == NULL)
    {
      if (errno == ENOENT)
        {
          return SUCCESS;
        }
      error_causef (e, ERR_IO, "opendir %s: %s", path, strerror (errno));
      return error_trace (e);
    }

  struct dirent *entry;
  char child[PATH_MAX];

  while ((entry = readdir (dir)) != NULL)
    {
      if (strcmp (entry->d_name, ".") == 0 || strcmp (entry->d_name, "..") == 0)
        {
          continue;
        }
      snprintf (child, sizeof child, "%s/%s", path, entry->d_name);
      if (unlink (child) && errno != ENOENT)
        {
          closedir (dir);
          error_causef (e, ERR_IO, "unlink %s: %s", child, strerror (errno));
          return error_trace (e);
        }
    }

  closedir (dir);

  if (rmdir (path) && errno != ENOENT)
    {
      error_causef (e, ERR_IO, "rmdir %s: %s", path, strerror (errno));
      return error_trace (e);
    }

  return SUCCESS;
}

i64
i_seek (const i_file *fp, const u64 offset, const seek_t whence, error *e)
{
  int seek;
  switch (whence)
    {
    case I_SEEK_SET:
      {
        seek = SEEK_SET;
        break;
      }
    case I_SEEK_CUR:
      {
        seek = SEEK_CUR;
        break;
      }
    case I_SEEK_END:
      {
        seek = SEEK_END;
        break;
      }
    default:
      {
        UNREACHABLE ();
      }
    }

  errno = 0;
  const off_t ret = lseek (fp->fd, offset, seek);
  if (ret == (off_t)-1)
    {
      error_causef (e, ERR_IO, "lseek: %s", strerror (errno));
      return error_trace (e);
    }

  return (u64)ret;
}

////////////////////////////////////////////////////////////
// WRAPPERS
err_t
i_access_rw (const char *fname, error *e)
{
  if (access (fname, F_OK | W_OK | R_OK))
    {
      error_causef (e, ERR_IO, "access: %s", strerror (errno));
      return error_trace (e);
    }
  return SUCCESS;
}

bool
i_exists_rw (const char *fname)
{
  if (access (fname, F_OK | W_OK | R_OK))
    {
      return false;
    }
  return true;
}

err_t
i_touch (const char *fname, error *e)
{
  ASSERT (fname);

  i_file fd = { 0 };
  WRAP (i_open_rw (&fd, fname, e));
  WRAP (i_close (&fd, e));

  return SUCCESS;
}

err_t
i_dir_exists (const char *fname, bool *dest, error *e)
{
  struct stat statbuf;
  if (stat (fname, &statbuf) != 0)
    {
      if (ENOENT == errno)
        {
          *dest = false;
          return 0;
        }
      else
        {
          error_causef (e, ERR_IO, "stat %s: %s", fname, strerror (errno));
          return error_trace (e);
        }
    }

  *dest = S_ISDIR (statbuf.st_mode);

  return SUCCESS;
}

err_t
i_file_exists (const char *fname, bool *dest, error *e)
{
  struct stat statbuf;
  if (stat (fname, &statbuf) != 0)
    {
      if (ENOENT == errno)
        {
          *dest = false;
          return 0;
        }
      else
        {
          error_causef (e, ERR_IO, "stat %s: %s", fname, strerror (errno));
          return error_trace (e);
        }
    }

  *dest = S_ISREG (statbuf.st_mode);

  return SUCCESS;
}
