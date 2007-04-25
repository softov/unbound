/* 
 * util/data/msgparse.c - parse wireformat DNS messages.
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
 * "AS IS" AND ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT LIMITED
 * TO, THE IMPLIED WARRANTIES OF MERCHANTABILITY AND FITNESS FOR A PARTICULAR
 * PURPOSE ARE DISCLAIMED. IN NO EVENT SHALL THE REGENTS OR CONTRIBUTORS BE
 * LIABLE FOR ANY DIRECT, INDIRECT, INCIDENTAL, SPECIAL, EXEMPLARY, OR
 * CONSEQUENTIAL DAMAGES (INCLUDING, BUT NOT LIMITED TO, PROCUREMENT OF
 * SUBSTITUTE GOODS OR SERVICES; LOSS OF USE, DATA, OR PROFITS; OR BUSINESS
 * INTERRUPTION) HOWEVER CAUSED AND ON ANY THEORY OF LIABILITY, WHETHER IN
 * CONTRACT, STRICT LIABILITY, OR TORT (INCLUDING NEGLIGENCE OR OTHERWISE)
 * ARISING IN ANY WAY OUT OF THE USE OF THIS SOFTWARE, EVEN IF ADVISED OF THE
 * POSSIBILITY OF SUCH DAMAGE.
 */
/**
 * \file
 * Routines for message parsing a packet buffer to a descriptive structure.
 */
#include "config.h"
#include "util/data/msgparse.h"
#include "util/net_help.h"
#include "util/data/dname.h"
#include "util/data/packed_rrset.h"
#include "util/storage/lookup3.h"
#include "util/region-allocator.h"

/** smart comparison of (compressed, valid) dnames from packet. */
static int
smart_compare(ldns_buffer* pkt, uint8_t* dnow, 
	uint8_t* dprfirst, uint8_t* dprlast)
{
	if(LABEL_IS_PTR(*dnow)) {
		/* ptr points to a previous dname */
		uint8_t* p = ldns_buffer_at(pkt, PTR_OFFSET(dnow[0], dnow[1]));
		if( p == dprfirst || p == dprlast )
			return 0;
		/* prev dname is also a ptr, both ptrs are the same. */
		if(LABEL_IS_PTR(*dprlast) &&
			dprlast[0] == dnow[0] && dprlast[1] == dnow[1])
			return 0;
	}
	return dname_pkt_compare(pkt, dnow, dprlast);
}

/**
 * Allocate new rrset in region, fill with data.
 */
static struct rrset_parse* 
new_rrset(struct msg_parse* msg, uint8_t* dname, size_t dnamelen, 
	uint16_t type, uint16_t dclass, hashvalue_t hash, 
	uint32_t rrset_flags, ldns_pkt_section section, region_type* region)
{
	struct rrset_parse* p = region_alloc(region, sizeof(*p));
	if(!p) return NULL;
	p->rrset_bucket_next = msg->hashtable[hash & (PARSE_TABLE_SIZE-1)];
	msg->hashtable[hash & (PARSE_TABLE_SIZE-1)] = p;
	p->rrset_all_next = 0;
	if(msg->rrset_last)
		msg->rrset_last->rrset_all_next = p;
	else 	msg->rrset_first = p;
	msg->rrset_last = p;
	p->hash = hash;
	p->section = section;
	p->dname = dname;
	p->dname_len = dnamelen;
	p->type = type;
	p->rrset_class = dclass;
	p->flags = rrset_flags;
	p->rr_count = 0;
	p->size = 0;
	p->rr_first = 0;
	p->rr_last = 0;
	p->rrsig_count = 0;
	p->rrsig_first = 0;
	p->rrsig_last = 0;
	return p;
}

/** See if next rrset is nsec at zone apex. */
static int
nsec_at_apex(ldns_buffer* pkt)
{
	/* we are at ttl position in packet. */
	size_t pos = ldns_buffer_position(pkt);
	uint16_t rdatalen;
	if(ldns_buffer_remaining(pkt) < 7) /* ttl+len+root */
		return 0; /* eek! */
	ldns_buffer_skip(pkt, 4); /* ttl */;
	rdatalen = ldns_buffer_read_u16(pkt);
	if(ldns_buffer_remaining(pkt) < rdatalen) {
		ldns_buffer_set_position(pkt, pos);
		return 0; /* parse error happens later */
	}
	/* must validate the nsec next domain name format */
	if(pkt_dname_len(pkt) == 0) {
		ldns_buffer_set_position(pkt, pos);
		return 0; /* parse error */
	}

	/* see if SOA bit is set. */
	if(ldns_buffer_position(pkt) < pos+rdatalen) {
		/* nsec type bitmap contains items */
		uint8_t win, blen, bits;
		/* need: windownum, bitmap len, firstbyte */
		if(ldns_buffer_position(pkt)+3 > pos+rdatalen) {
			ldns_buffer_set_position(pkt, pos);
			return 0; /* malformed nsec */
		}
		win = ldns_buffer_read_u8(pkt);
		blen = ldns_buffer_read_u8(pkt);
		bits = ldns_buffer_read_u8(pkt);
		/* 0window always first window. bitlen >=1 or parse
		   error really. bit 0x2 is SOA. */
		if(win == 0 && blen >= 1 && (bits & 0x02)) {
			ldns_buffer_set_position(pkt, pos);
			return 1;
		}
	}

	ldns_buffer_set_position(pkt, pos);
	return 0;
}

/** Calculate rrset flags */
static uint32_t
pkt_rrset_flags(struct msg_parse* msg, ldns_buffer* pkt, uint16_t type)
{
	uint32_t f;
	if(msg->flags & BIT_CD)
		f = PACKED_RRSET_CD;
	else	f = 0;
	if(type == LDNS_RR_TYPE_NSEC && nsec_at_apex(pkt)) {
		f |= PACKED_RRSET_NSEC_AT_APEX;
	}
	return f;
}

/** Calculate hash value for rrset in packet. */
static hashvalue_t
pkt_hash_rrset(ldns_buffer* pkt, uint8_t* dname, uint16_t type, 
	uint16_t dclass, uint32_t rrset_flags)
{
	hashvalue_t h = 0xab;
	h = hashlittle(&type, sizeof(type), h);
	h = hashlittle(&dclass, sizeof(dclass), h);
	h = hashlittle(&rrset_flags, sizeof(uint32_t), h);
	h = dname_pkt_hash(pkt, dname, h);
	return h;
}

/** compare rrset_parse with data. */
static int
rrset_parse_equals(struct rrset_parse* p, ldns_buffer* pkt, hashvalue_t h, 
	uint32_t rrset_flags, uint8_t* dname, size_t dnamelen, 
	uint16_t type, uint16_t dclass)
{
	if(p->hash == h && p->dname_len == dnamelen && p->type == type &&
		p->rrset_class == dclass && p->flags == rrset_flags &&
		dname_pkt_compare(pkt, dname, p->dname) == 0)
		return 1;
	return 0;
}


/**
 * Lookup in msg hashtable to find a rrset
 */
static struct rrset_parse*
hashtable_lookup(struct msg_parse* msg, ldns_buffer* pkt, hashvalue_t h, 
	uint32_t rrset_flags, uint8_t* dname, size_t dnamelen, 
	uint16_t type, uint16_t dclass)
{
	struct rrset_parse* p = msg->hashtable[h & (PARSE_TABLE_SIZE-1)];
	while(p) {
		if(rrset_parse_equals(p, pkt, h, rrset_flags, dname, dnamelen,
			type, dclass))
			return p;
		p = p->rrset_bucket_next;
	}
	return NULL;
}

/** return type networkformat that rrsig in packet covers */
static int
pkt_rrsig_covered(ldns_buffer* pkt, uint8_t* here, uint16_t* type)
{
	size_t pos = ldns_buffer_position(pkt);
	ldns_buffer_set_position(pkt, (size_t)(here-ldns_buffer_begin(pkt)));
	/* ttl + len + size of small rrsig(rootlabel, no signature) */
	if(ldns_buffer_remaining(pkt) < 4+2+19)
		return 0;
	ldns_buffer_skip(pkt, 4); /* ttl */
	if(ldns_buffer_read_u16(pkt) < 19) /* too short */ {
		ldns_buffer_set_position(pkt, pos);
		return 0;
	}
	*type = ldns_buffer_read_u16(pkt);
	ldns_buffer_set_position(pkt, pos);
	return 1;
}

/** true if covered type equals prevtype */
static int
pkt_rrsig_covered_equals(ldns_buffer* pkt, uint8_t* here, uint16_t type)
{
	uint16_t t;
	if(pkt_rrsig_covered(pkt, here, &t) && t == type)
		return 1;
	return 0;
}

/** remove rrset from hash list */
static void
bucket_remove(struct msg_parse* msg, struct rrset_parse* rrset)
{
	struct rrset_parse** p;
	p = &msg->hashtable[ rrset->hash & (PARSE_TABLE_SIZE-1) ];
	while(*p) {
		if(*p == rrset) {
			*p = rrset->rrset_bucket_next;
			return;
		}
		p = &( (*p)->rrset_bucket_next );
	}
}

/** change section of rrset from previous to current section */
static void
change_section(struct msg_parse* msg, struct rrset_parse* rrset,
	ldns_pkt_section section)
{
	struct rrset_parse *p, *prev;
	/* remove from list */
	if(section == rrset->section)
		return;
	p = msg->rrset_first;
	prev = 0;
	while(p) {
		if(p == rrset) {
			if(prev) prev->rrset_all_next = p->rrset_all_next;
			else	msg->rrset_first = p->rrset_all_next;
			if(msg->rrset_last == rrset)
				msg->rrset_last = prev;
			break;
		}
		prev = p;
		p = p->rrset_all_next;
	}
	/* remove from count */
	switch(rrset->section) {
		case LDNS_SECTION_ANSWER: msg->an_rrsets--; break;
		case LDNS_SECTION_AUTHORITY: msg->ns_rrsets--; break;
		case LDNS_SECTION_ADDITIONAL: msg->ar_rrsets--; break;
		default: log_assert(0);
	}
	/* insert at end of list */
	rrset->rrset_all_next = 0;
	if(msg->rrset_last)
		msg->rrset_last->rrset_all_next = rrset;
	else	msg->rrset_first = rrset;
	msg->rrset_last = rrset;
	/* up count of new section */
	switch(section) {
		case LDNS_SECTION_AUTHORITY: msg->ns_rrsets++; break;
		case LDNS_SECTION_ADDITIONAL: msg->ar_rrsets++; break;
		default: log_assert(0);
	}
	rrset->section = section;
}

/** see if rrset of type RRSIG contains sig over given type */
static int
rrset_has_sigover(ldns_buffer* pkt, struct rrset_parse* rrset, uint16_t type,
	int* hasother)
{
	int res = 0;
	struct rr_parse* rr = rrset->rr_first;
	log_assert( rrset->type == LDNS_RR_TYPE_RRSIG );
	while(rr) {
		if(pkt_rrsig_covered_equals(pkt, rr->ttl_data, type))
			res = 1;
		else	*hasother = 1;
		rr = rr->next;
	}
	return res;
}

/** move rrsigs from sigset to dataset */
static int
moveover_rrsigs(ldns_buffer* pkt, region_type* region, 
	struct rrset_parse* sigset, struct rrset_parse* dataset, int duplicate)
{
	struct rr_parse* sig = sigset->rr_first;
	struct rr_parse* prev = NULL;
	struct rr_parse* insert;
	while(sig) {
		if(pkt_rrsig_covered_equals(pkt, sig->ttl_data, 
			dataset->type)) {
			if(duplicate) {
				/* new */
				insert = (struct rr_parse*)region_alloc(region,
					sizeof(struct rr_parse));
				insert->ttl_data = sig->ttl_data;
				insert->size = sig->size;
			} else {
				/* remove from sigset */
				if(prev) prev->next = sig->next;
				else	sigset->rr_first = sig->next;
				if(sigset->rr_last == sig)
					sigset->rr_last = prev;
				sigset->rr_count--;
				sigset->size -= sig->size;
				insert = sig;
			}
			/* add to dataset */
			dataset->rrsig_count++;
			insert->next = 0;
			if(dataset->rrsig_last) 
				dataset->rrsig_last->next = insert;
			else	dataset->rrsig_first = insert;
			dataset->rrsig_last = insert;
			dataset->size += insert->size;
		}
		prev = sig;
		sig = sig->next;
	}
	return 1;
}

/** change an rrsig rrset for use as data rrset */
static struct rrset_parse*
change_rrsig_rrset(struct rrset_parse* sigset, struct msg_parse* msg, 
	ldns_buffer* pkt, uint16_t datatype, uint32_t rrset_flags,
	int hasother, ldns_pkt_section section, region_type* region)
{
	struct rrset_parse* dataset = sigset;
	hashvalue_t hash = pkt_hash_rrset(pkt, sigset->dname, datatype, 
		sigset->rrset_class, rrset_flags);
	log_assert( sigset->type == LDNS_RR_TYPE_RRSIG );
	log_assert( datatype != LDNS_RR_TYPE_RRSIG );
	if(hasother) {
		/* need to make new rrset to hold data type */
		dataset = new_rrset(msg, sigset->dname, sigset->dname_len, 
			datatype, sigset->rrset_class, hash, rrset_flags, 
			section, region);
		if(!dataset) 
			return NULL;
		switch(section) {
			case LDNS_SECTION_ANSWER: msg->an_rrsets++; break;
			case LDNS_SECTION_AUTHORITY: msg->ns_rrsets++; break;
			case LDNS_SECTION_ADDITIONAL: msg->ar_rrsets++; break;
			default: log_assert(0);
		}
		if(!moveover_rrsigs(pkt, region, sigset, dataset, 
			msg->qtype == LDNS_RR_TYPE_RRSIG ||
			msg->qtype == LDNS_RR_TYPE_ANY ))
			return NULL;
		return dataset;
	}
	/* changeover the type of the rrset to data set */
	bucket_remove(msg, dataset);
	/* insert into new hash bucket */
	dataset->rrset_bucket_next = msg->hashtable[hash&(PARSE_TABLE_SIZE-1)];
	msg->hashtable[hash&(PARSE_TABLE_SIZE-1)] = dataset;
	dataset->hash = hash;
	/* use section of data item for result */
	change_section(msg, dataset, section);
	dataset->type = datatype;
	dataset->flags = rrset_flags;
	dataset->rrsig_count += dataset->rr_count;
	dataset->rr_count = 0;
	/* move sigs to end of siglist */
	if(dataset->rrsig_last)
		dataset->rrsig_last->next = dataset->rr_first;
	else	dataset->rrsig_first = dataset->rr_first;
	dataset->rrsig_last = dataset->rr_last;
	return dataset;
}

/** Find rrset. If equal to previous it is fast. hash if not so.
 * @param msg: the message with hash table.
 * @param pkt: the packet in wireformat (needed for compression ptrs).
 * @param dname: pointer to start of dname (compressed) in packet.
 * @param dnamelen: uncompressed wirefmt length of dname.
 * @param type: type of current rr.
 * @param dclass: class of current rr.
 * @param hash: hash value is returned if the rrset could not be found.
 * @param rrset_flags: is returned if the rrset could not be found.
 * @param prev_dname_first: dname of last seen RR. First seen dname.
 * @param prev_dname_last: dname of last seen RR. Last seen dname.
 * @param prev_dnamelen: dname len of last seen RR.
 * @param prev_type: type of last seen RR.
 * @param prev_dclass: class of last seen RR.
 * @param rrset_prev: last seen RRset.
 * @param section: the current section in the packet.
 * @param region: used to allocate temporary parsing data.
 * @return 0 on out of memory.
 */
static int
find_rrset(struct msg_parse* msg, ldns_buffer* pkt, uint8_t* dname, 
	size_t dnamelen, uint16_t type, uint16_t dclass, hashvalue_t* hash, 
	uint32_t* rrset_flags,
	uint8_t** prev_dname_first, uint8_t** prev_dname_last,
	size_t* prev_dnamelen, uint16_t* prev_type,
	uint16_t* prev_dclass, struct rrset_parse** rrset_prev,
	ldns_pkt_section section, region_type* region)
{
	uint16_t covtype;
	if(rrset_prev) {
		/* check if equal to previous item */
		if(type == *prev_type && dclass == *prev_dclass &&
			dnamelen == *prev_dnamelen &&
			smart_compare(pkt, dname, *prev_dname_first, 
				*prev_dname_last) == 0) {
			/* same as previous */
			*prev_dname_last = dname;
			return 1;
		}
		/* check if rrsig over previous item */
		if(type == LDNS_RR_TYPE_RRSIG && dclass == *prev_dclass &&
			pkt_rrsig_covered_equals(pkt, ldns_buffer_current(pkt),
				*prev_type) &&
			smart_compare(pkt, dname, *prev_dname_first,
				*prev_dname_last) == 0) {
			/* covers previous */
			*prev_dname_last = dname;
			return 1;
		}
	}
	/* find by hashing and lookup in hashtable */
	*rrset_flags = pkt_rrset_flags(msg, pkt, type);
	
	/* if rrsig - try to lookup matching data set first */
	if(type == LDNS_RR_TYPE_RRSIG && pkt_rrsig_covered(pkt, 
		ldns_buffer_current(pkt), &covtype)) {
		*hash = pkt_hash_rrset(pkt, dname, covtype, dclass, 
			*rrset_flags);
		*rrset_prev = hashtable_lookup(msg, pkt, *hash, *rrset_flags, 
			dname, dnamelen, covtype, dclass);
		if(!*rrset_prev && covtype == LDNS_RR_TYPE_NSEC) {
			/* if NSEC try with NSEC apex bit twiddled */
			*rrset_flags ^= PACKED_RRSET_NSEC_AT_APEX;
			*hash = pkt_hash_rrset(pkt, dname, covtype, dclass, 
				*rrset_flags);
			*rrset_prev = hashtable_lookup(msg, pkt, *hash, 
				*rrset_flags, dname, dnamelen, covtype, dclass);
		}
		if(*rrset_prev) {
			*prev_dname_first = (*rrset_prev)->dname;
			*prev_dname_last = dname;
			*prev_dnamelen = dnamelen;
			*prev_type = covtype;
			*prev_dclass = dclass;
			return 1;
		}
	}
	if(type != LDNS_RR_TYPE_RRSIG) {
		int hasother = 0;
		/* find matching rrsig */
		*hash = pkt_hash_rrset(pkt, dname, LDNS_RR_TYPE_RRSIG, 
			dclass, *rrset_flags);
		*rrset_prev = hashtable_lookup(msg, pkt, *hash, *rrset_flags, 
			dname, dnamelen, LDNS_RR_TYPE_RRSIG, dclass);
		if(*rrset_prev && rrset_has_sigover(pkt, *rrset_prev, type,
			&hasother)) {
			/* yes! */
			*prev_dname_first = (*rrset_prev)->dname;
			*prev_dname_last = dname;
			*prev_dnamelen = dnamelen;
			*prev_type = type;
			*prev_dclass = dclass;
			*rrset_prev = change_rrsig_rrset(*rrset_prev, msg, 
				pkt, type, *rrset_flags, hasother, section, 
				region);
			if(!*rrset_prev) return 0;
			return 1;
		}
	}

	*hash = pkt_hash_rrset(pkt, dname, type, dclass, *rrset_flags);
	*rrset_prev = hashtable_lookup(msg, pkt, *hash, *rrset_flags, 
		dname, dnamelen, type, dclass);
	if(*rrset_prev)
		*prev_dname_first = (*rrset_prev)->dname;
	else 	*prev_dname_first = dname;
	*prev_dname_last = dname;
	*prev_dnamelen = dnamelen;
	*prev_type = type;
	*prev_dclass = dclass;
	return 1;
}

/**
 * Parse query section. 
 * @param pkt: packet, position at call must be at start of query section.
 *	at end position is after query section.
 * @param msg: store results here.
 * @return: 0 if OK, or rcode on error.
 */
static int
parse_query_section(ldns_buffer* pkt, struct msg_parse* msg)
{
	if(msg->qdcount == 0)
		return 0;
	if(msg->qdcount > 1)
		return LDNS_RCODE_FORMERR;
	log_assert(msg->qdcount == 1);
	if(ldns_buffer_remaining(pkt) <= 0)
		return LDNS_RCODE_FORMERR;
	msg->qname = ldns_buffer_current(pkt);
	if((msg->qname_len = query_dname_len(pkt)) == 0)
		return LDNS_RCODE_FORMERR;
	if(ldns_buffer_remaining(pkt) < sizeof(uint16_t)*2)
		return LDNS_RCODE_FORMERR;
	msg->qtype = ldns_buffer_read_u16(pkt);
	msg->qclass = ldns_buffer_read_u16(pkt);
	return 0;
}

size_t
get_rdf_size(ldns_rdf_type rdf)
{
	switch(rdf) {
		case LDNS_RDF_TYPE_CLASS:
		case LDNS_RDF_TYPE_ALG:
		case LDNS_RDF_TYPE_INT8:
			return 1;
			break;
		case LDNS_RDF_TYPE_INT16:
		case LDNS_RDF_TYPE_TYPE:
		case LDNS_RDF_TYPE_CERT_ALG:
			return 2;
			break;
		case LDNS_RDF_TYPE_INT32:
		case LDNS_RDF_TYPE_TIME:
		case LDNS_RDF_TYPE_A:
		case LDNS_RDF_TYPE_PERIOD:
			return 4;
			break;
		case LDNS_RDF_TYPE_TSIGTIME:
			return 6;
			break;
		case LDNS_RDF_TYPE_AAAA:
			return 16;
			break;
		default:
			log_assert(false); /* add type above */
			/* only types that appear before a domain  *
			 * name are needed. rest is simply copied. */
	}
	return 0;
}

/** calculate the size of one rr */
static int
calc_size(ldns_buffer* pkt, uint16_t type, struct rr_parse* rr)
{
	const ldns_rr_descriptor* desc;
	uint16_t pkt_len; /* length of rr inside the packet */
	rr->size = sizeof(uint16_t); /* the rdatalen */
	ldns_buffer_skip(pkt, 4); /* skip ttl */
	pkt_len = ldns_buffer_read_u16(pkt);
	if(ldns_buffer_remaining(pkt) < pkt_len)
		return 0;
	desc = ldns_rr_descript(type);
	if(pkt_len > 0 && desc && desc->_dname_count > 0) {
		int count = (int)desc->_dname_count;
		int rdf = 0;
		size_t len;
		size_t oldpos;
		/* skip first part. */
		while(pkt_len > 0 && count) {
			switch(desc->_wireformat[rdf]) {
			case LDNS_RDF_TYPE_DNAME:
				/* decompress every domain name */
				oldpos = ldns_buffer_position(pkt);
				if((len = pkt_dname_len(pkt)) == 0)
					return 0; /* malformed dname */
				if(ldns_buffer_position(pkt)-oldpos > pkt_len)
					return 0; /* dname exceeds rdata */
				pkt_len -= ldns_buffer_position(pkt)-oldpos;
				rr->size += len;
				count--;
				len = 0;
				break;
			case LDNS_RDF_TYPE_STR:
				if(pkt_len < 1)
					return 0; /* len byte exceeds rdata */
				len = ldns_buffer_current(pkt)[0] + 1;
				break;
			default:
				len = get_rdf_size(desc->_wireformat[rdf]);
			}
			if(len) {
				if(pkt_len < len)
					return 0; /* exceeds rdata */
				pkt_len -= len;
				ldns_buffer_skip(pkt, (ssize_t)len);
				rr->size += len;
			}
			rdf++;
		}
	}
	/* remaining rdata */
	rr->size += pkt_len;
	ldns_buffer_skip(pkt, (ssize_t)pkt_len);
	return 1;
}

/** skip rr ttl and rdata */
static int
skip_ttl_rdata(ldns_buffer* pkt) 
{
	uint16_t rdatalen;
	if(ldns_buffer_remaining(pkt) < 6) /* ttl + rdatalen */
		return 0;
	ldns_buffer_skip(pkt, 4); /* ttl */
	rdatalen = ldns_buffer_read_u16(pkt);
	if(ldns_buffer_remaining(pkt) < rdatalen)
		return 0;
	ldns_buffer_skip(pkt, (ssize_t)rdatalen);
	return 1;
}

/** see if RRSIG is a duplicate of another */
static int
sig_is_double(ldns_buffer* pkt, struct rrset_parse* rrset, uint8_t* ttldata)
{
	uint16_t rlen, siglen;
	size_t pos = ldns_buffer_position(pkt);
	struct rr_parse* sig;
	if(ldns_buffer_remaining(pkt) < 6) 
		return 0;
	ldns_buffer_skip(pkt, 4); /* ttl */
	rlen = ldns_buffer_read_u16(pkt);
	if(ldns_buffer_remaining(pkt) < rlen) {
		ldns_buffer_set_position(pkt, pos);
		return 0;
	}
	ldns_buffer_set_position(pkt, pos);

	sig = rrset->rrsig_first;
	while(sig) {
		/* check if rdatalen is same */
		memmove(&siglen, sig->ttl_data+4, sizeof(siglen));
		siglen = ntohs(siglen);
		/* checks if data in packet is exactly the same, this means
		 * also dname in rdata is the same, but rrsig is not allowed
		 * to have compressed dnames anyway. If it is compressed anyway
		 * it will lead to duplicate rrs for qtype=RRSIG. (or ANY).
		 *
		 * Cannot use sig->size because size of the other one is not 
		 * calculated yet.
		 */
		if(siglen == rlen) {
			if(siglen>0 && memcmp(sig->ttl_data+6, ttldata+6, 
				siglen) == 0) {
				/* same! */
				return 1;
			}
		}
		sig = sig->next;
	}
	return 0;
}

/** Add rr (from packet here) to rrset, skips rr */
static int
add_rr_to_rrset(struct rrset_parse* rrset, ldns_buffer* pkt, 
	struct msg_parse* msg, region_type* region, 
	ldns_pkt_section section, uint16_t type)
{
	struct rr_parse* rr;
	/* check section of rrset. */
	if(rrset->section != section && type != LDNS_RR_TYPE_RRSIG &&
		rrset->type != LDNS_RR_TYPE_RRSIG) {
		/* silently drop it - it is a security problem, since
		 * trust in rr data depends on the section it is in. 
		 * the less trustworthy part is discarded. */
		verbose(VERB_DETAIL, "Packet contains rrset data in "
			"multiple sections, dropped last part.");
		/* forwards */
		if(!skip_ttl_rdata(pkt))
			return LDNS_RCODE_FORMERR;
		return 0;
	} 

	if( (msg->qtype == LDNS_RR_TYPE_RRSIG ||
	     msg->qtype == LDNS_RR_TYPE_ANY) 
	    && sig_is_double(pkt, rrset, ldns_buffer_current(pkt))) {
		if(!skip_ttl_rdata(pkt))
			return LDNS_RCODE_FORMERR;
		return 0;
	}

	/* create rr */
	if(!(rr = (struct rr_parse*)region_alloc(region, sizeof(*rr))))
		return LDNS_RCODE_SERVFAIL;
	rr->ttl_data = ldns_buffer_current(pkt);
	rr->next = 0;
	if(type == LDNS_RR_TYPE_RRSIG) {
		if(rrset->rrsig_last) 
			rrset->rrsig_last->next = rr;
		else	rrset->rrsig_first = rr;
		rrset->rrsig_last = rr;
		rrset->rrsig_count++;
	} else {
		if(rrset->rr_last)
			rrset->rr_last->next = rr;
		else	rrset->rr_first = rr;
		rrset->rr_last = rr;
		rrset->rr_count++;
	}

	/* calc decompressed size */
	if(!calc_size(pkt, type, rr))
		return LDNS_RCODE_FORMERR;
	rrset->size += rr->size;

	return 0;
}

/**
 * Parse packet RR section, for answer, authority and additional sections. 
 * @param pkt: packet, position at call must be at start of section.
 *	at end position is after section.
 * @param msg: store results here.
 * @param region: how to alloc results.
 * @param section: section enum.
 * @param num_rrs: how many rrs are in the section.
 * @param num_rrsets: returns number of rrsets in the section.
 * @return: 0 if OK, or rcode on error.
 */
static int
parse_section(ldns_buffer* pkt, struct msg_parse* msg, region_type* region,
	ldns_pkt_section section, uint16_t num_rrs, size_t* num_rrsets)
{
	uint16_t i;
	uint8_t* dname, *prev_dname_f = NULL, *prev_dname_l = NULL;
	size_t dnamelen, prev_dnamelen = 0;
	uint16_t type, prev_type = 0;
	uint16_t dclass, prev_dclass = 0;
	uint32_t rrset_flags = 0;
	hashvalue_t hash = 0;
	struct rrset_parse* rrset = NULL;
	int r;

	if(num_rrs == 0)
		return 0;
	if(ldns_buffer_remaining(pkt) <= 0)
		return LDNS_RCODE_FORMERR;
	for(i=0; i<num_rrs; i++) {
		/* parse this RR. */
		dname = ldns_buffer_current(pkt);
		if((dnamelen = pkt_dname_len(pkt)) == 0)
			return LDNS_RCODE_FORMERR;
		if(ldns_buffer_remaining(pkt) < 10) /* type, class, ttl, len */
			return LDNS_RCODE_FORMERR;
		type = ldns_buffer_read_u16(pkt);
		ldns_buffer_read(pkt, &dclass, sizeof(dclass));

		if(0) { /* debug show what is being parsed. */
			printf("parse of %s(%d)",
				ldns_rr_descript(type)?
				ldns_rr_descript(type)->_name: "??",
				(int)type);
			printf(" %s(%d) ",
				ldns_lookup_by_id(ldns_rr_classes, 
				(int)ntohs(dclass))?ldns_lookup_by_id(
				ldns_rr_classes, (int)ntohs(dclass))->name: 
				"??", (int)ntohs(dclass));
			dname_print(stdout, pkt, dname);
			printf("\n");
		}

		/* see if it is part of an existing RR set */
		if(!find_rrset(msg, pkt, dname, dnamelen, type, dclass, &hash, 
			&rrset_flags, &prev_dname_f, &prev_dname_l, 
			&prev_dnamelen, &prev_type, &prev_dclass, &rrset, 
			section, region))
			return LDNS_RCODE_SERVFAIL;
		if(!rrset) {
			/* it is a new RR set. hash&flags already calculated.*/
			(*num_rrsets)++;
			rrset = new_rrset(msg, dname, dnamelen, type, dclass,
				hash, rrset_flags, section, region);
			if(!rrset) 
				return LDNS_RCODE_SERVFAIL;
		}
		/* add to rrset. */
		if((r=add_rr_to_rrset(rrset, pkt, msg, region, section, 
			type)) != 0)
			return r;
	}
	return 0;
}

int
parse_packet(ldns_buffer* pkt, struct msg_parse* msg, region_type* region)
{
	int ret;
	if(ldns_buffer_remaining(pkt) < LDNS_HEADER_SIZE)
		return LDNS_RCODE_FORMERR;
	/* read the header */
	ldns_buffer_read(pkt, &msg->id, sizeof(uint16_t));
	msg->flags = ldns_buffer_read_u16(pkt);
	msg->qdcount = ldns_buffer_read_u16(pkt);
	msg->ancount = ldns_buffer_read_u16(pkt);
	msg->nscount = ldns_buffer_read_u16(pkt);
	msg->arcount = ldns_buffer_read_u16(pkt);
	if(msg->qdcount > 1)
		return LDNS_RCODE_FORMERR;
	if((ret = parse_query_section(pkt, msg)) != 0)
		return ret;
	if((ret = parse_section(pkt, msg, region, LDNS_SECTION_ANSWER,
		msg->ancount, &msg->an_rrsets)) != 0)
		return ret;
	if((ret = parse_section(pkt, msg, region, LDNS_SECTION_AUTHORITY,
		msg->nscount, &msg->ns_rrsets)) != 0)
		return ret;
	if((ret = parse_section(pkt, msg, region, LDNS_SECTION_ADDITIONAL, 
		msg->arcount, &msg->ar_rrsets)) != 0)
		return ret;
	/* if(ldns_buffer_remaining(pkt) > 0) { */
		/* there is spurious data at end of packet. ignore */
	/* } */
	msg->rrset_count = msg->an_rrsets + msg->ns_rrsets + msg->ar_rrsets;
	return 0;
}

int 
parse_extract_edns(struct msg_parse* msg, struct edns_data* edns)
{
	struct rrset_parse* rrset = msg->rrset_first;
	struct rrset_parse* prev = 0;
	struct rrset_parse* found = 0;
	struct rrset_parse* found_prev = 0;
	/* since the class encodes the UDP size, we cannot use hash table to
	 * find the EDNS OPT record. Scan the packet. */
	while(rrset) {
		if(rrset->type == LDNS_RR_TYPE_OPT) {
			/* only one OPT RR allowed. */
			if(found) return LDNS_RCODE_FORMERR;
			/* found it! */
			found_prev = prev;
			found = rrset;
		}
		prev = rrset;
		rrset = rrset->rrset_all_next;
	}
	if(!found) {
		memset(edns, 0, sizeof(*edns));
		edns->udp_size = 512;
		return 0;
	}
	/* check the found RRset */
	/* most lenient check possible. ignore dname, use last opt */
	if(found->section != LDNS_SECTION_ADDITIONAL)
		return LDNS_RCODE_FORMERR; 
	if(found->rr_count == 0)
		return LDNS_RCODE_FORMERR;
	if(0) { /* strict checking of dname and RRcount */
		if(found->dname_len != 1 || !found->dname 
			|| found->dname[0] != 0) return LDNS_RCODE_FORMERR; 
		if(found->rr_count != 1) return LDNS_RCODE_FORMERR; 
	}
	log_assert(found->rr_first == found->rr_last && found->rr_first);

	/* remove from packet */
	if(found_prev)	found_prev->rrset_all_next = found->rrset_all_next;
	else	msg->rrset_first = found->rrset_all_next;
	if(found == msg->rrset_last)
		msg->rrset_last = found_prev;
	msg->arcount --;
	msg->ar_rrsets --;
	msg->rrset_count --;
	
	/* take the data ! */
	edns->edns_present = 1;
	edns->ext_rcode = found->rr_last->ttl_data[0];
	edns->edns_version = found->rr_last->ttl_data[1];
	edns->bits = ldns_read_uint16(&found->rr_last->ttl_data[2]);
	edns->udp_size = ntohs(found->rrset_class);
	/* ignore rdata and rrsigs */
	return 0;
}
