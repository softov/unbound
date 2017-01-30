/*
 * util/config_file.c - reads and stores the config file for unbound.
 *
 * Copyright (c) 2007, NLnet Labs. All rights reserved.
 *
 * This software is open source.
 * 
 * Redistribution and use in source and binary forms, with or without
 * modification, are permitted provided that the following conditions
 * are met:
 * 
 * Redistributions of source code must retain the above copyright notice,
 * this list of conditions and the following disclaimer.
 * 
 * Redistributions in binary form must reproduce the above copyright notice,
 * this list of conditions and the following disclaimer in the documentation
 * and/or other materials provided with the distribution.
 * 
 * Neither the name of the NLNET LABS nor the names of its contributors may
 * be used to endorse or promote products derived from this software without
 * specific prior written permission.
 * 
 * THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS
 * "AS IS" AND ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT
 * LIMITED TO, THE IMPLIED WARRANTIES OF MERCHANTABILITY AND FITNESS FOR
 * A PARTICULAR PURPOSE ARE DISCLAIMED. IN NO EVENT SHALL THE COPYRIGHT
 * HOLDER OR CONTRIBUTORS BE LIABLE FOR ANY DIRECT, INDIRECT, INCIDENTAL,
 * SPECIAL, EXEMPLARY, OR CONSEQUENTIAL DAMAGES (INCLUDING, BUT NOT LIMITED
 * TO, PROCUREMENT OF SUBSTITUTE GOODS OR SERVICES; LOSS OF USE, DATA, OR
 * PROFITS; OR BUSINESS INTERRUPTION) HOWEVER CAUSED AND ON ANY THEORY OF
 * LIABILITY, WHETHER IN CONTRACT, STRICT LIABILITY, OR TORT (INCLUDING
 * NEGLIGENCE OR OTHERWISE) ARISING IN ANY WAY OUT OF THE USE OF THIS
 * SOFTWARE, EVEN IF ADVISED OF THE POSSIBILITY OF SUCH DAMAGE.
 */

/**
 * \file
 *
 * This file contains functions for the config file.
 */

/************************************************************/
#include "shm_main.h"
/************************************************************/
/** subtract timers and the values do not overflow or become negative */
static void
timeval_subtract(struct timeval* d, const struct timeval* end,
	const struct timeval* start)
{
#ifndef S_SPLINT_S
	time_t end_usec = end->tv_usec;
	d->tv_sec = end->tv_sec - start->tv_sec;
	if(end_usec < start->tv_usec) {
		end_usec += 1000000;
		d->tv_sec--;
	}
	d->tv_usec = end_usec - start->tv_usec;
#endif
}
/************************************************************/
int shm_main_init(struct daemon* daemon)
{
	struct shm_stat_info *shm_stat;
	struct timeval now;
	int shm_size;
	time_t t;
	
	/* sanitize */
	if(!daemon)
		return 0;

	/* Statistics to maintain the number of thread + total */
	shm_size							= (sizeof(struct stats_info) * (daemon->num + 1));

	/* Allocation of needed memory */
	daemon->shm_info 					= (struct shm_main_info*)calloc(1, shm_size);

	/* Sanitize */
	if(!daemon->shm_info)
		return 0;

	daemon->shm_info->key				= daemon->cfg->shm_key;

	/* Check for previous create SHM */
	daemon->shm_info->id_ctl 			= shmget(daemon->shm_info->key, sizeof(int), SHM_R);
	daemon->shm_info->id_arr 			= shmget(daemon->shm_info->key + 1, sizeof(int), SHM_R);

	/* Destroy previous SHM */
	if (daemon->shm_info->id_ctl >= 0)
		shmctl(daemon->shm_info->id_ctl, IPC_RMID, NULL);

	/* Destroy previous SHM */
	if (daemon->shm_info->id_arr >= 0)
		shmctl(daemon->shm_info->id_ctl, IPC_RMID, NULL);

	/* SHM: Create the segment */
	daemon->shm_info->id_ctl 			= shmget(daemon->shm_info->key, sizeof(struct shm_stat_info), IPC_CREAT | 0666);

	if (daemon->shm_info->id_ctl < 0)
	{
		log_warn("SHM FAIL - Can't do the SHM GET - KEY [%d] - ERROR [%d]", daemon->shm_info->key, errno);

		/* Just release memory unused */
		free(daemon->shm_info);

		return 0;
	}

	daemon->shm_info->id_arr 			= shmget(daemon->shm_info->key + 1, shm_size, IPC_CREAT | 0666);

	if (daemon->shm_info->id_arr < 0)
	{
		log_warn("SHM FAIL - Can't do the SHM GET - KEY [%d] - ERROR [%d]", daemon->shm_info->key, errno);

		/* Just release memory unused */
		free(daemon->shm_info);

		return 0;
	}

	/* SHM: attach the segment  */
	daemon->shm_info->ptr_ctl 			= shmat(daemon->shm_info->id_ctl, NULL, 0);
	daemon->shm_info->ptr_arr 			= shmat(daemon->shm_info->id_arr, NULL, 0);

	if ((daemon->shm_info->ptr_ctl == (void *) -1) || (daemon->shm_info->ptr_arr == (void *) -1))
	{
		log_warn("SHM FAIL - Attach has failed on ID [%d]-[%d] - ERROR [%d]\n", daemon->shm_info->id_ctl, daemon->shm_info->id_arr, errno);

		/* Just release memory unused */
		free(daemon->shm_info);

		return 0;
	}

	/* Zero fill SHM to stand clean while is not filled by other events */
	memset(daemon->shm_info->ptr_ctl, 0, sizeof(struct shm_stat_info));
	memset(daemon->shm_info->ptr_arr, 0, shm_size);

	shm_stat 							= (struct shm_stat_info *)daemon->shm_info->ptr_ctl;
	shm_stat->num_threads				= daemon->num;

	return 1;
}
/************************************************************/
int shm_main_shutdown(struct daemon* daemon)
{
	/* web are OK, just disabled */
	if(!daemon->cfg->shm_enable)
		return 1;

	log_info("SHM SHUTDOWN - KEY [%d] - ID CTL [%d] ARR [%d] - PTR CTL [%p] ARR [%p]",
			daemon->shm_info->key, daemon->shm_info->id_ctl, daemon->shm_info->id_arr, daemon->shm_info->ptr_ctl, daemon->shm_info->ptr_arr);

	/* Destroy previous SHM */
	if (daemon->shm_info->id_ctl >= 0)
		shmctl(daemon->shm_info->id_ctl, IPC_RMID, NULL);

	if (daemon->shm_info->id_arr >= 0)
		shmctl(daemon->shm_info->id_arr, IPC_RMID, NULL);

	if (daemon->shm_info->ptr_ctl)
		shmdt(daemon->shm_info->ptr_ctl);

	if (daemon->shm_info->ptr_arr)
		shmdt(daemon->shm_info->ptr_arr);

	return 1;
}
/************************************************************/
int shm_main_run(struct worker *worker)
{
	struct shm_stat_info *shm_stat;
	struct stats_info *stat_total;
	struct stats_info *stat_info;
	int modstack;
	int offset;

	log_info("SHM RUN - worker [%d] - daemon [%p] - TIME [%d] - [%d]",
			worker->thread_num, worker->daemon, worker->env.now_tv->tv_sec, worker->daemon->time_boot.tv_sec);

	offset						= ((worker->thread_num + 1) * sizeof(struct stats_info));
	stat_total 					= (struct stats_info *)(worker->daemon->shm_info->ptr_arr);
	stat_info					= (struct stats_info *)(worker->daemon->shm_info->ptr_arr + offset);

	/* Copy data to the current position */
	server_stats_compile(worker, stat_info, 0);

	/* First thread, zero fill total, and copy general info */
	if (worker->thread_num == 0) {

		/* Copy data to the current position */
		memset(stat_total, 0, sizeof(struct stats_info));

		/* Point to data into SHM */
		shm_stat 					= (struct shm_stat_info *)worker->daemon->shm_info->ptr_ctl;
		shm_stat->time.now 			= *worker->env.now_tv;

		timeval_subtract(&shm_stat->time.up, &shm_stat->time.now, &worker->daemon->time_boot);
		timeval_subtract(&shm_stat->time.elapsed, &shm_stat->time.now, &worker->daemon->time_last_stat);

		shm_stat->mem.msg 			= slabhash_get_mem(worker->env.msg_cache);
		shm_stat->mem.rrset 		= slabhash_get_mem(&worker->env.rrset_cache->table);
		shm_stat->mem.val 			= 0;
		shm_stat->mem.iter			= 0;

		modstack 					= modstack_find(&worker->env.mesh->mods, "validator");
		if(modstack != -1) {
			fptr_ok(fptr_whitelist_mod_get_mem(worker->env.mesh->mods.mod[modstack]->get_mem));
			shm_stat->mem.val 		= (*worker->env.mesh->mods.mod[modstack]->get_mem)(&worker->env, modstack);
		}
		modstack 					= modstack_find(&worker->env.mesh->mods, "iterator");
		if(modstack != -1) {
			fptr_ok(fptr_whitelist_mod_get_mem(worker->env.mesh->mods.mod[modstack]->get_mem));
			shm_stat->mem.iter 		= (*worker->env.mesh->mods.mod[modstack]->get_mem)(&worker->env, modstack);
		}
	}

	server_stats_add(stat_total, stat_info);

	/* print the thread statistics */
	stat_total->mesh_time_median 	/= (double)worker->daemon->num;

	return 1;
}
/************************************************************/
