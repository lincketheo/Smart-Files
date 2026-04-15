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

#define WIN32_LEAN_AND_MEAN
#include "tlclib/dev/assert.h"
#include "tlclib/error.h"
#include "tlclib/intf/logging.h"
#include "tlclib/intf/os/file_system.h"
#include "tlclib/windows.h"

#include <stdio.h>
#include <string.h>

////////////////////////////////////////////////////////////
// Helpers

static char *
win32_strerror (DWORD err, char *buf, DWORD buflen)
{
  FormatMessageA (FORMAT_MESSAGE_FROM_SYSTEM | FORMAT_MESSAGE_IGNORE_INSERTS,
                  NULL, err, 0, buf, buflen, NULL);
  return buf;
}

#define WIN_ERR_BUF 256
#define WIN_ERRMSG(buf) win32_strerror (GetLastError (), buf, sizeof (buf))

#ifndef NDEBUG
static bool
handle_is_open (HANDLE h)
{
  return h != NULL && h != INVALID_HANDLE_VALUE;
}
#endif

DEFINE_DBG_ASSERT (i_file, i_file, fp, {
  ASSERT (fp);
  ASSERT (handle_is_open (fp->handle));
})

////////////////////////////////////////////////////////////
// OPEN / CLOSE

err_t
i_open_rw (i_file *dest, const char *fname, error *e)
{
  HANDLE h = CreateFileA (fname, GENERIC_READ | GENERIC_WRITE,
                          FILE_SHARE_READ | FILE_SHARE_WRITE, NULL, OPEN_ALWAYS,
                          FILE_ATTRIBUTE_NORMAL, NULL);
  if (h == INVALID_HANDLE_VALUE)
    {
      char buf[WIN_ERR_BUF];
      return error_causef (e, ERR_IO, "open_rw %s: %s", fname, WIN_ERRMSG (buf));
    }
  dest->handle = h;
  DBG_ASSERT (i_file, dest);
  return SUCCESS;
}

err_t
i_open_r (i_file *dest, const char *fname, error *e)
{
  HANDLE h = CreateFileA (fname, GENERIC_READ, FILE_SHARE_READ, NULL,
                          OPEN_ALWAYS, FILE_ATTRIBUTE_NORMAL, NULL);
  if (h == INVALID_HANDLE_VALUE)
    {
      char buf[WIN_ERR_BUF];
      return error_causef (e, ERR_IO, "open_r %s: %s", fname, WIN_ERRMSG (buf));
    }
  dest->handle = h;
  DBG_ASSERT (i_file, dest);
  return SUCCESS;
}

err_t
i_open_w (i_file *dest, const char *fname, error *e)
{
  HANDLE h = CreateFileA (fname, GENERIC_WRITE, FILE_SHARE_WRITE, NULL,
                          OPEN_ALWAYS, FILE_ATTRIBUTE_NORMAL, NULL);
  if (h == INVALID_HANDLE_VALUE)
    {
      char buf[WIN_ERR_BUF];
      return error_causef (e, ERR_IO, "open_w %s: %s", fname, WIN_ERRMSG (buf));
    }
  dest->handle = h;
  DBG_ASSERT (i_file, dest);
  return SUCCESS;
}

err_t
i_close (const i_file *fp, error *e)
{
  DBG_ASSERT (i_file, fp);
  if (!CloseHandle (fp->handle))
    {
      char buf[WIN_ERR_BUF];
      return error_causef (e, ERR_IO, "close: %s", WIN_ERRMSG (buf));
    }
  fp->handle = INVALID_HANDLE_VALUE;
  return SUCCESS;
}

err_t
i_eof (i_file *fp, error *e)
{
  // No direct equivalent on Windows — no-op for API symmetry
  (void)fp;
  (void)e;
  return SUCCESS;
}

err_t
i_fsync (const i_file *fp, error *e)
{
  DBG_ASSERT (i_file, fp);
  if (!FlushFileBuffers (fp->handle))
    {
      char buf[WIN_ERR_BUF];
      return error_causef (e, ERR_IO, "fsync: %s", WIN_ERRMSG (buf));
    }
  return SUCCESS;
}

////////////////////////////////////////////////////////////
// Positional Read / Write
//
// Even on non-overlapped handles, Windows honors Offset/OffsetHigh
// in OVERLAPPED for positional I/O without moving the file pointer.

static OVERLAPPED
make_overlapped (u64 offset)
{
  OVERLAPPED ov = { 0 };
  ov.Offset = (DWORD)(offset & 0xFFFFFFFFULL);
  ov.OffsetHigh = (DWORD)(offset >> 32);
  return ov;
}

i64
i_pread_some (const i_file *fp, void *dest, u64 n, u64 offset, error *e)
{
  DBG_ASSERT (i_file, fp);
  ASSERT (dest);
  ASSERT (n > 0);

  OVERLAPPED ov = make_overlapped (offset);
  DWORD nread = 0;

  if (!ReadFile (fp->handle, dest, (DWORD)n, &nread, &ov))
    {
      DWORD err = GetLastError ();
      if (err == ERROR_HANDLE_EOF)
        return 0;
      char buf[WIN_ERR_BUF];
      win32_strerror (err, buf, sizeof (buf));
      return error_causef (e, ERR_IO, "pread: %s", buf);
    }
  return (i64)nread;
}

i64
i_pread_all (const i_file *fp, void *dest, u64 n, u64 offset, error *e)
{
  DBG_ASSERT (i_file, fp);
  ASSERT (dest);
  ASSERT (n > 0);

  u8 *_dest = (u8 *)dest;
  u64 nread = 0;

  while (nread < n)
    {
      OVERLAPPED ov = make_overlapped (offset + nread);
      DWORD chunk = 0;
      DWORD want = (DWORD)((n - nread) > 0xFFFFFFFFULL ? 0xFFFFFFFFUL : (n - nread));

      if (!ReadFile (fp->handle, _dest + nread, want, &chunk, &ov))
        {
          DWORD err = GetLastError ();
          if (err == ERROR_HANDLE_EOF)
            return (i64)nread;
          char buf[WIN_ERR_BUF];
          win32_strerror (err, buf, sizeof (buf));
          return error_causef (e, ERR_IO, "pread: %s", buf);
        }

      if (chunk == 0)
        return (i64)nread; // EOF

      nread += chunk;
    }

  ASSERT (nread == n);
  return (i64)nread;
}

err_t
i_pread_all_expect (i_file *fp, void *dest, u64 n, u64 offset, error *e)
{
  i64 ret = i_pread_all (fp, dest, n, offset, e);
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
i_pwrite_some (const i_file *fp, const void *src, u64 n, u64 offset,
               error *e)
{
  DBG_ASSERT (i_file, fp);
  ASSERT (src);
  ASSERT (n > 0);

  OVERLAPPED ov = make_overlapped (offset);
  DWORD nwritten = 0;

  if (!WriteFile (fp->handle, src, (DWORD)n, &nwritten, &ov))
    {
      char buf[WIN_ERR_BUF];
      return error_causef (e, ERR_IO, "pwrite: %s", WIN_ERRMSG (buf));
    }
  return (i64)nwritten;
}

err_t
i_pwrite_all (const i_file *fp, const void *src, u64 n, u64 offset,
              error *e)
{
  DBG_ASSERT (i_file, fp);
  ASSERT (src);
  ASSERT (n > 0);

  const u8 *_src = (const u8 *)src;
  u64 nwrite = 0;

  while (nwrite < n)
    {
      OVERLAPPED ov = make_overlapped (offset + nwrite);
      DWORD chunk = 0;
      DWORD want = (DWORD)((n - nwrite) > 0xFFFFFFFFULL ? 0xFFFFFFFFUL : (n - nwrite));

      if (!WriteFile (fp->handle, _src + nwrite, want, &chunk, &ov))
        {
          char buf[WIN_ERR_BUF];
          return error_causef (e, ERR_IO, "pwrite: %s", WIN_ERRMSG (buf));
        }
      nwrite += chunk;
    }

  ASSERT (nwrite == n);
  return SUCCESS;
}

////////////////////////////////////////////////////////////
// IO Vec (no scatter-gather on Windows for regular files — loop)

i64
i_writev_some (const i_file *fp, const struct bytes *src, int iovcnt,
               error *e)
{
  DBG_ASSERT (i_file, fp);
  ASSERT (src);
  ASSERT (iovcnt > 0);
  ASSERT (iovcnt <= 2);

  i64 total = 0;
  for (int i = 0; i < iovcnt; i++)
    {
      DWORD nwritten = 0;
      if (!WriteFile (fp->handle, src[i].head, (DWORD)src[i].len, &nwritten,
                      NULL))
        {
          char buf[WIN_ERR_BUF];
          return error_causef (e, ERR_IO, "writev: %s", WIN_ERRMSG (buf));
        }
      total += nwritten;
      if ((u64)nwritten < src[i].len)
        break; // partial
    }
  return total;
}

err_t
i_writev_all (const i_file *fp, struct bytes *iov, int iovcnt, error *e)
{
  DBG_ASSERT (i_file, fp);
  ASSERT (iov);
  ASSERT (iovcnt > 0);
  ASSERT (iovcnt <= 2);

  for (int i = 0; i < iovcnt; i++)
    {
      u8 *src = (u8 *)iov[i].head;
      u64 remain = iov[i].len;
      while (remain > 0)
        {
          DWORD want = (DWORD)(remain > 0xFFFFFFFFULL ? 0xFFFFFFFFUL : remain);
          DWORD nwritten = 0;
          if (!WriteFile (fp->handle, src, want, &nwritten, NULL))
            {
              char buf[WIN_ERR_BUF];
              return error_causef (e, ERR_IO, "writev: %s", WIN_ERRMSG (buf));
            }
          src += nwritten;
          remain -= nwritten;
        }
    }
  return SUCCESS;
}

i64
i_readv_some (const i_file *fp, const struct bytes *iov, int iovcnt,
              error *e)
{
  DBG_ASSERT (i_file, fp);
  ASSERT (iov);
  ASSERT (iovcnt > 0);
  ASSERT (iovcnt <= 2);

  i64 total = 0;
  for (int i = 0; i < iovcnt; i++)
    {
      DWORD nread = 0;
      if (!ReadFile (fp->handle, iov[i].head, (DWORD)iov[i].len, &nread, NULL))
        {
          DWORD err = GetLastError ();
          if (err == ERROR_HANDLE_EOF)
            break;
          char buf[WIN_ERR_BUF];
          win32_strerror (err, buf, sizeof (buf));
          return error_causef (e, ERR_IO, "readv: %s", buf);
        }
      total += nread;
      if ((u64)nread < iov[i].len)
        break; // partial / EOF
    }
  return total;
}

i64
i_readv_all (const i_file *fp, struct bytes *iov, int iovcnt, error *e)
{
  DBG_ASSERT (i_file, fp);
  ASSERT (iov);
  ASSERT (iovcnt > 0);
  ASSERT (iovcnt <= 2);

  i64 total = 0;
  for (int i = 0; i < iovcnt; i++)
    {
      u8 *dst = (u8 *)iov[i].head;
      u64 remain = iov[i].len;
      while (remain > 0)
        {
          DWORD want = (DWORD)(remain > 0xFFFFFFFFULL ? 0xFFFFFFFFUL : remain);
          DWORD nread = 0;
          if (!ReadFile (fp->handle, dst, want, &nread, NULL))
            {
              DWORD err = GetLastError ();
              if (err == ERROR_HANDLE_EOF)
                goto done;
              char buf[WIN_ERR_BUF];
              win32_strerror (err, buf, sizeof (buf));
              return error_causef (e, ERR_IO, "readv: %s", buf);
            }
          if (nread == 0)
            goto done; // EOF
          dst += nread;
          remain -= nread;
          total += nread;
        }
    }
done:
  return total;
}

////////////////////////////////////////////////////////////
// Stream Read / Write

i64
i_read_some (const i_file *fp, void *dest, u64 nbytes, error *e)
{
  DBG_ASSERT (i_file, fp);
  ASSERT (dest);
  ASSERT (nbytes > 0);

  DWORD nread = 0;
  if (!ReadFile (fp->handle, dest, (DWORD)nbytes, &nread, NULL))
    {
      DWORD err = GetLastError ();
      if (err == ERROR_HANDLE_EOF)
        return 0;
      char buf[WIN_ERR_BUF];
      win32_strerror (err, buf, sizeof (buf));
      return error_causef (e, ERR_IO, "read: %s", buf);
    }
  return (i64)nread;
}

i64
i_read_all (const i_file *fp, void *dest, u64 nbytes, error *e)
{
  DBG_ASSERT (i_file, fp);
  ASSERT (dest);
  ASSERT (nbytes > 0);

  u8 *_dest = (u8 *)dest;
  u64 nread = 0;

  while (nread < nbytes)
    {
      DWORD chunk = 0;
      DWORD want = (DWORD)((nbytes - nread) > 0xFFFFFFFFULL ? 0xFFFFFFFFUL
                                                            : (nbytes - nread));

      if (!ReadFile (fp->handle, _dest + nread, want, &chunk, NULL))
        {
          DWORD err = GetLastError ();
          if (err == ERROR_HANDLE_EOF)
            return (i64)nread;
          char buf[WIN_ERR_BUF];
          win32_strerror (err, buf, sizeof (buf));
          return error_causef (e, ERR_IO, "read: %s", buf);
        }

      if (chunk == 0)
        return (i64)nread; // EOF

      nread += chunk;
    }

  ASSERT (nread == nbytes);
  return (i64)nread;
}

i64
i_read_all_expect (i_file *fp, void *dest, u64 nbytes, error *e)
{
  i64 ret = i_read_all (fp, dest, nbytes, e);
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
i_write_some (const i_file *fp, const void *src, u64 nbytes, error *e)
{
  DBG_ASSERT (i_file, fp);
  ASSERT (src);
  ASSERT (nbytes > 0);

  DWORD nwritten = 0;
  if (!WriteFile (fp->handle, src, (DWORD)nbytes, &nwritten, NULL))
    {
      char buf[WIN_ERR_BUF];
      return error_causef (e, ERR_IO, "write: %s", WIN_ERRMSG (buf));
    }
  return (i64)nwritten;
}

err_t
i_write_all (const i_file *fp, const void *src, u64 nbytes, error *e)
{
  DBG_ASSERT (i_file, fp);
  ASSERT (src);
  ASSERT (nbytes > 0);

  const u8 *_src = (const u8 *)src;
  u64 nwrite = 0;

  while (nwrite < nbytes)
    {
      DWORD chunk = 0;
      DWORD want = (DWORD)((nbytes - nwrite) > 0xFFFFFFFFULL ? 0xFFFFFFFFUL
                                                             : (nbytes - nwrite));

      if (!WriteFile (fp->handle, _src + nwrite, want, &chunk, NULL))
        {
          char buf[WIN_ERR_BUF];
          return error_causef (e, ERR_IO, "write: %s", WIN_ERRMSG (buf));
        }
      nwrite += chunk;
    }

  ASSERT (nwrite == nbytes);
  return SUCCESS;
}

////////////////////////////////////////////////////////////
// Others

err_t
i_truncate (const i_file *fp, u64 bytes, error *e)
{
  DBG_ASSERT (i_file, fp);

  LARGE_INTEGER li;
  li.QuadPart = (LONGLONG)bytes;

  if (!SetFilePointerEx (fp->handle, li, NULL, FILE_BEGIN))
    {
      char buf[WIN_ERR_BUF];
      return error_causef (e, ERR_IO, "truncate (seek): %s", WIN_ERRMSG (buf));
    }
  if (!SetEndOfFile (fp->handle))
    {
      char buf[WIN_ERR_BUF];
      return error_causef (e, ERR_IO, "truncate: %s", WIN_ERRMSG (buf));
    }
  return SUCCESS;
}

err_t
i_fallocate (i_file *fp, u64 bytes, error *e)
{
  // Windows has no posix_fallocate — extend via SetEndOfFile.
  // For true preallocation, DeviceIoControl(FSCTL_SET_SPARSE) can be used,
  // but SetEndOfFile + no sparse flag forces physical allocation on NTFS.
  DBG_ASSERT (i_file, fp);

  LARGE_INTEGER li;
  li.QuadPart = (LONGLONG)bytes;

  if (!SetFilePointerEx (fp->handle, li, NULL, FILE_BEGIN))
    {
      char buf[WIN_ERR_BUF];
      return error_causef (e, ERR_IO, "fallocate (seek): %s", WIN_ERRMSG (buf));
    }
  if (!SetEndOfFile (fp->handle))
    {
      char buf[WIN_ERR_BUF];
      return error_causef (e, ERR_IO, "fallocate: %s", WIN_ERRMSG (buf));
    }
  return SUCCESS;
}

i64
i_file_size (const i_file *fp, error *e)
{
  DBG_ASSERT (i_file, fp);

  LARGE_INTEGER size;
  if (!GetFileSizeEx (fp->handle, &size))
    {
      char buf[WIN_ERR_BUF];
      error_causef (e, ERR_IO, "fstat: %s", WIN_ERRMSG (buf));
      return error_trace (e);
    }
  return (i64)size.QuadPart;
}

err_t
i_remove_quiet (const char *fname, error *e)
{
  if (!DeleteFileA (fname))
    {
      DWORD err = GetLastError ();
      if (err != ERROR_FILE_NOT_FOUND && err != ERROR_PATH_NOT_FOUND)
        {
          char buf[WIN_ERR_BUF];
          win32_strerror (err, buf, sizeof (buf));
          error_causef (e, ERR_IO, "remove: %s", buf);
          return error_trace (e);
        }
    }
  return SUCCESS;
}

err_t
i_mkstemp (i_file *dest, char *tmpl, error *e)
{
  char tmp_dir[MAX_PATH];
  char tmp_path[MAX_PATH];

  GetTempPathA (MAX_PATH, tmp_dir);
  if (!GetTempFileNameA (tmp_dir, "", 0, tmp_path))
    {
      char buf[WIN_ERR_BUF];
      error_causef (e, ERR_IO, "mkstemp: %s", WIN_ERRMSG (buf));
      return error_trace (e);
    }

  strncpy (tmpl, tmp_path, MAX_PATH - 1);
  tmpl[MAX_PATH - 1] = '\0';

  // GetTempFileName already creates the file; re-open it with our flags
  HANDLE h = CreateFileA (tmp_path, GENERIC_READ | GENERIC_WRITE, 0, NULL,
                          CREATE_ALWAYS, FILE_ATTRIBUTE_TEMPORARY, NULL);
  if (h == INVALID_HANDLE_VALUE)
    {
      char buf[WIN_ERR_BUF];
      error_causef (e, ERR_IO, "mkstemp (open): %s", WIN_ERRMSG (buf));
      return error_trace (e);
    }

  dest->handle = h;
  return SUCCESS;
}

err_t
i_unlink (const char *name, error *e)
{
  if (!DeleteFileA (name))
    {
      char buf[WIN_ERR_BUF];
      error_causef (e, ERR_IO, "unlink: %s", WIN_ERRMSG (buf));
      return error_trace (e);
    }
  return SUCCESS;
}

err_t
i_mkdir (const char *name, error *e)
{
  if (!CreateDirectoryA (name, NULL))
    {
      char buf[WIN_ERR_BUF];
      error_causef (e, ERR_IO, "mkdir: %s", WIN_ERRMSG (buf));
      return error_trace (e);
    }
  return SUCCESS;
}

err_t
i_mkdir_quiet (const char *name, error *e)
{
  if (CreateDirectoryA (name, NULL))
    return SUCCESS;

  DWORD err = GetLastError ();
  if (err != ERROR_ALREADY_EXISTS)
    {
      char buf[WIN_ERR_BUF];
      win32_strerror (err, buf, sizeof (buf));
      error_causef (e, ERR_IO, "mkdir: %s", buf);
      return error_trace (e);
    }

  DWORD attrs = GetFileAttributesA (name);
  if (attrs == INVALID_FILE_ATTRIBUTES)
    {
      char buf[WIN_ERR_BUF];
      error_causef (e, ERR_IO, "mkdir_quiet (stat): %s", WIN_ERRMSG (buf));
      return error_trace (e);
    }

  if (!(attrs & FILE_ATTRIBUTE_DIRECTORY))
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
  char pattern[MAX_PATH];
  snprintf (pattern, sizeof (pattern), "%s\\*", path);

  WIN32_FIND_DATAA fd;
  HANDLE hFind = FindFirstFileA (pattern, &fd);

  if (hFind == INVALID_HANDLE_VALUE)
    {
      DWORD err = GetLastError ();
      if (err == ERROR_FILE_NOT_FOUND || err == ERROR_PATH_NOT_FOUND)
        return SUCCESS;
      char buf[WIN_ERR_BUF];
      win32_strerror (err, buf, sizeof (buf));
      error_causef (e, ERR_IO, "opendir %s: %s", path, buf);
      return error_trace (e);
    }

  do
    {
      if (strcmp (fd.cFileName, ".") == 0 || strcmp (fd.cFileName, "..") == 0)
        continue;

      char child[MAX_PATH];
      snprintf (child, sizeof (child), "%s\\%s", path, fd.cFileName);

      if (!DeleteFileA (child))
        {
          DWORD err = GetLastError ();
          if (err != ERROR_FILE_NOT_FOUND)
            {
              FindClose (hFind);
              char buf[WIN_ERR_BUF];
              win32_strerror (err, buf, sizeof (buf));
              error_causef (e, ERR_IO, "unlink %s: %s", child, buf);
              return error_trace (e);
            }
        }
    }
  while (FindNextFileA (hFind, &fd));

  FindClose (hFind);

  if (!RemoveDirectoryA (path))
    {
      DWORD err = GetLastError ();
      if (err != ERROR_FILE_NOT_FOUND && err != ERROR_PATH_NOT_FOUND)
        {
          char buf[WIN_ERR_BUF];
          win32_strerror (err, buf, sizeof (buf));
          error_causef (e, ERR_IO, "rmdir %s: %s", path, buf);
          return error_trace (e);
        }
    }

  return SUCCESS;
}

i64
i_seek (const i_file *fp, u64 offset, seek_t whence, error *e)
{
  DBG_ASSERT (i_file, fp);

  DWORD method;
  switch (whence)
    {
    case I_SEEK_SET:
      method = FILE_BEGIN;
      break;
    case I_SEEK_CUR:
      method = FILE_CURRENT;
      break;
    case I_SEEK_END:
      method = FILE_END;
      break;
    default:
      UNREACHABLE ();
    }

  LARGE_INTEGER li, result;
  li.QuadPart = (LONGLONG)offset;

  if (!SetFilePointerEx (fp->handle, li, &result, method))
    {
      char buf[WIN_ERR_BUF];
      error_causef (e, ERR_IO, "lseek: %s", WIN_ERRMSG (buf));
      return error_trace (e);
    }

  return (i64)result.QuadPart;
}

////////////////////////////////////////////////////////////
// Wrappers

err_t
i_access_rw (const char *fname, error *e)
{
  if (GetFileAttributesA (fname) == INVALID_FILE_ATTRIBUTES)
    {
      char buf[WIN_ERR_BUF];
      error_causef (e, ERR_IO, "access: %s", WIN_ERRMSG (buf));
      return error_trace (e);
    }
  return SUCCESS;
}

bool
i_exists_rw (const char *fname)
{
  return GetFileAttributesA (fname) != INVALID_FILE_ATTRIBUTES;
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
  DWORD attrs = GetFileAttributesA (fname);
  if (attrs == INVALID_FILE_ATTRIBUTES)
    {
      DWORD err = GetLastError ();
      if (err == ERROR_FILE_NOT_FOUND || err == ERROR_PATH_NOT_FOUND)
        {
          *dest = false;
          return SUCCESS;
        }
      char buf[WIN_ERR_BUF];
      win32_strerror (err, buf, sizeof (buf));
      error_causef (e, ERR_IO, "stat %s: %s", fname, buf);
      return error_trace (e);
    }
  *dest = (attrs & FILE_ATTRIBUTE_DIRECTORY) != 0;
  return SUCCESS;
}

err_t
i_file_exists (const char *fname, bool *dest, error *e)
{
  DWORD attrs = GetFileAttributesA (fname);
  if (attrs == INVALID_FILE_ATTRIBUTES)
    {
      DWORD err = GetLastError ();
      if (err == ERROR_FILE_NOT_FOUND || err == ERROR_PATH_NOT_FOUND)
        {
          *dest = false;
          return SUCCESS;
        }
      char buf[WIN_ERR_BUF];
      win32_strerror (err, buf, sizeof (buf));
      error_causef (e, ERR_IO, "stat %s: %s", fname, buf);
      return error_trace (e);
    }
  *dest = !(attrs & FILE_ATTRIBUTE_DIRECTORY);
  return SUCCESS;
}
