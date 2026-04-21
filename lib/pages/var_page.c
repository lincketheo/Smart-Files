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

#include "pages/var_page.h"

#include "c_specx.h"
#include "pages/page.h"
#include "smfile.h"

// numstore

DEFINE_DBG_ASSERT (page, vp_page, v, { ASSERT (v); })

////////////////////////////////////////////////////////////
// INITIALIZATION

void
vp_init_empty (page *p)
{
  ASSERT (page_get_type (p) == PG_VAR_PAGE);
  vp_set_next (p, PGNO_NULL);
  vp_set_ovnext (p, PGNO_NULL);
  vp_set_vlen (p, 0);
  vp_set_root (p, PGNO_NULL);
}

#ifndef NTEST
TEST (vp_init_empty)
{
  page p;

  rand_bytes (p.raw, PAGE_SIZE);
  page_init_empty (&p, PG_VAR_PAGE);

  test_assert_equal (vp_get_next (&p), PGNO_NULL);
  test_assert_equal (vp_get_ovnext (&p), PGNO_NULL);
  test_assert_equal (vp_get_vlen (&p), 0);
  test_assert_equal (vp_get_root (&p), PGNO_NULL);
  test_assert_equal (vp_calc_tofst (&p), VP_VNME_OFST);
  test_assert_equal (vp_is_overflow (&p), false);
}
#endif

////////////////////////////////////////////////////////////
// SETTERS

void
vp_set_next (page *p, const pgno pg)
{
  PAGE_SIMPLE_SET_IMPL (p, pg, VP_NEXT_OFST);
}

void
vp_set_ovnext (page *p, const pgno pg)
{
  PAGE_SIMPLE_SET_IMPL (p, pg, VP_OVNX_OFST);
}

void
vp_set_vlen (page *p, const u16 vlen)
{
  PAGE_SIMPLE_SET_IMPL (p, vlen, VP_VLEN_OFST);
}

void
vp_set_root (page *p, const pgno root)
{
  PAGE_SIMPLE_SET_IMPL (p, root, VP_ROOT_OFST);
}

void
vp_set_nbytes (page *p, const b_size nbytes)
{
  PAGE_SIMPLE_SET_IMPL (p, nbytes, VP_NBYT_OFST);
}

void vp_append_cbuffer (page *p, struct cbuffer src);

////////////////////////////////////////////////////////////
// GETTERS

pgno
vp_get_next (const page *p)
{
  PAGE_SIMPLE_GET_IMPL (p, pgno, VP_NEXT_OFST);
}

pgno
vp_get_ovnext (const page *p)
{
  PAGE_SIMPLE_GET_IMPL (p, pgno, VP_OVNX_OFST);
}

u16
vp_get_vlen (const page *p)
{
  PAGE_SIMPLE_GET_IMPL (p, u16, VP_VLEN_OFST);
}

pgno
vp_get_root (const page *p)
{
  PAGE_SIMPLE_GET_IMPL (p, pgno, VP_ROOT_OFST);
}

b_size
vp_get_nbytes (const page *p)
{
  PAGE_SIMPLE_GET_IMPL (p, b_size, VP_NBYT_OFST);
}

b_size
vp_calc_tofst (const page *p)
{
  const b_size vlen = (b_size)vp_get_vlen (p);
  return (b_size)VP_VNME_OFST + vlen;
}

bool
vp_is_overflow (const page *p)
{
  return vp_calc_tofst (p) > PAGE_SIZE;
}

struct bytes
vp_get_bytes (page *p)
{
  return (struct bytes){
    .head = (void *)&p->raw[VP_VNME_OFST],
    .len = PAGE_SIZE - VP_VNME_OFST,
  };
}

struct cbytes
vp_get_bytes_imut (const page *p)
{
  return (struct cbytes){
    .head = (void *)&p->raw[VP_VNME_OFST],
    .len = PAGE_SIZE - VP_VNME_OFST,
  };
}

////////////////////////////////////////////////////////////
// VALIDATION

err_t
vp_validate_for_db (const page *p, error *e)
{
  if (page_get_type (p) != PG_VAR_PAGE)
    {
      return error_causef (e, ERR_CORRUPT, "wrong page type for var page");
    }
  if (vp_get_vlen (p) == 0)
    {
      return error_causef (e, ERR_CORRUPT, "var length is 0");
    }
  if (vp_is_overflow (p) && vp_get_ovnext (p) == PGNO_NULL)
    {
      return error_causef (e, ERR_CORRUPT,
                           "overflow required but next pointer is null");
    }
  if (vp_get_vlen (p) > MAX_VSTR)
    {
      return error_causef (e, ERR_CORRUPT, "vstring overflow");
    }
  return SUCCESS;
}

#ifndef NTEST
TEST (vp_validate)
{
  page sut;
  error e = error_create ();

  TEST_CASE ("Invalid page type")
  {
    page_init_empty (&sut, PG_DATA_LIST);
    vp_set_vlen (&sut, 10);
    test_err_t_check (vp_validate_for_db (&sut, &e), ERR_CORRUPT, &e);
  }

  TEST_CASE ("Empty variable length")
  {
    page_init_empty (&sut, PG_VAR_PAGE);
    vp_set_vlen (&sut, 0);
    test_err_t_check (vp_validate_for_db (&sut, &e), ERR_CORRUPT, &e);
  }

  TEST_CASE ("Empty type string length")
  {
    page_init_empty (&sut, PG_VAR_PAGE);
    vp_set_vlen (&sut, 0);
    test_err_t_check (vp_validate_for_db (&sut, &e), ERR_CORRUPT, &e);
  }

  TEST_CASE ("Overflow page requires next pointer")
  {
    page_init_empty (&sut, PG_VAR_PAGE);
    vp_set_vlen (&sut, PAGE_SIZE);
    vp_set_ovnext (&sut, PGNO_NULL);
    test_err_t_check (vp_validate_for_db (&sut, &e), ERR_CORRUPT, &e);
  }

  TEST_CASE ("Valid minimal varpage")
  {
    page_init_empty (&sut, PG_VAR_PAGE);
    vp_set_vlen (&sut, 1);
    test_err_t_check (vp_validate_for_db (&sut, &e), SUCCESS, &e);
  }

  TEST_CASE ("v string overflow")
  {
    page_init_empty (&sut, PG_VAR_PAGE);
    vp_set_vlen (&sut, MAX_VSTR + 1);
    test_err_t_check (vp_validate_for_db (&sut, &e), ERR_CORRUPT, &e);
    e.cause_code = SUCCESS;
    e.cmlen = 0;
  }

  TEST_CASE ("Valid overflow with next pointer")
  {
    page_init_empty (&sut, PG_VAR_PAGE);
    vp_set_vlen (&sut, 100);
    vp_set_next (&sut, 42);
    test_err_t_check (vp_validate_for_db (&sut, &e), SUCCESS, &e);
  }
}
#endif

////////////////////////////////////////////////////////////
// UTILS

void
i_log_vp (const int level, const page *vp)
{
  i_log (level, "=== VARIABLE PAGE START ===\n");

  i_printf (level, "PGNO:   %" PRpgno "\n", vp->pg);
  i_printf (level, "VLEN:   %u\n", vp_get_vlen (vp));
  if (vp_get_root (vp) == PGNO_NULL)
    {
      i_printf (level, "ROOT:  NULL\n");
    }
  else
    {
      i_printf (level, "ROOT:  %" PRpgno "\n", vp_get_root (vp));
    }
  if (vp_get_next (vp) == PGNO_NULL)
    {
      i_printf (level, "NEXT:  NULL\n");
    }
  else
    {
      i_printf (level, "NEXT:  %" PRpgno "\n", vp_get_next (vp));
    }
  if (vp_get_ovnext (vp) == PGNO_NULL)
    {
      i_printf (level, "OVNEXT:  NULL\n");
    }
  else
    {
      i_printf (level, "OVNEXT:  %" PRpgno "\n", vp_get_ovnext (vp));
    }
  i_printf (level, "TOFST:  %" PRb_size "\n", vp_calc_tofst (vp));

  i_log (level, "=== VARIABLE PAGE END ===\n");
}
