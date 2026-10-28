/*
 * Copyright (C) 2024-2026 Colin Ian King.
 *
 * This program is free software; you can redistribute it and/or
 * modify it under the terms of the GNU General Public License
 * as published by the Free Software Foundation; either version 2
 * of the License, or (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with this program; if not, write to the Free Software
 * Foundation, Inc., 51 Franklin Street, Fifth Floor, Boston, MA 02110-1301, USA.
 *
 */
#ifndef CORE_ASM_GENERIC_H
#define CORE_ASM_GENERIC_H

#include "core-arch.h"

static inline void ALWAYS_INLINE stress_asm_nop(void)
{
#if defined(HAVE_ASM_NOP)
#if defined(STRESS_ARCH_KVX)
	/*
	 * Extra ;; required for KVX to indicate end of
	 * a VLIW instruction bundle
	 */
	__asm__ __volatile__("nop\n;;\n");
#else
	__asm__ __volatile__("nop;\n");
#endif
#elif defined(HAVE_ASM_OPENRISC_NOP)
#define HAVE_ASM_NOP
	__asm__ __volatile__("l.nop;\n");
#endif
}

static inline void ALWAYS_INLINE stress_asm_mb(void)
{
#if defined(HAVE_ASM_MB)
        __asm__ __volatile__("" ::: "memory");
#endif
}

static inline void ALWAYS_INLINE stress_asm_nothing(void)
{
#if defined(HAVE_ASM_NOTHING)
	__asm__ __volatile__("");
#endif
}

#endif
