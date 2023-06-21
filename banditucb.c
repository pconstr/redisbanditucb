/* Redis module with a native data type implementing
 * the UCB algorithm for (non-contextual) multi-armed bandits
 */

#include "redismodule.h"
#include <stdio.h>
#include <stdlib.h>
#include <ctype.h>
#include <math.h>
#include <string.h>
#include <stdint.h>
#include <stdbool.h>

static RedisModuleType *BanditUCBType;

#define MAX_ARMS 64

typedef uint32_t ARM;
typedef uint64_t COUNT;

/* In-RAM data structure. counts and means have narms elements */
struct BanditUCBObject {
  ARM narms;
  double c; /* scaling constant for UCB */
  COUNT* counts;
  double* means;
};
typedef struct BanditUCBObject BanditUCBObject;

/* Create, only partially initialised. Counts and means need to be zero'd or filled */
BanditUCBObject *createBanditUCBObject(ARM narms, double c) {
    BanditUCBObject *o;
    o = RedisModule_Alloc(sizeof(*o));
    o->narms = narms;
    o->counts = RedisModule_Alloc(narms * sizeof(COUNT));
    o->means = RedisModule_Alloc(narms * sizeof(double));
    o->c = c;
    return o;
}


/* Zero counts and means */
void zeroBanditUCBObject(BanditUCBObject* o) {    
    for(uint32_t i = 0; i < o->narms; ++i)
      o->counts[i] = 0;
    for(uint32_t i = 0; i < o->narms; ++i)
      o->means[i] = 0.0;
}


/* Free memory */
void BanditUCBReleaseObject(BanditUCBObject *o) {
    RedisModule_Free(o->counts);
    RedisModule_Free(o->means);
    RedisModule_Free(o);
}


/* BANDITUCB.INIT <key> <narms> c
 * Returns number of arms */
int BanditUCBInit_RedisCommand(RedisModuleCtx *ctx, RedisModuleString **argv, int argc) {
    RedisModule_AutoMemory(ctx); /* Use automatic memory management. */

    if (argc != 4) return RedisModule_WrongArity(ctx);
    RedisModuleKey *key = RedisModule_OpenKey(ctx,argv[1],
        REDISMODULE_READ|REDISMODULE_WRITE);
    int type = RedisModule_KeyType(key);
    if (type != REDISMODULE_KEYTYPE_EMPTY &&
        RedisModule_ModuleTypeGetType(key) != BanditUCBType)
    {
        return RedisModule_ReplyWithError(ctx,REDISMODULE_ERRORMSG_WRONGTYPE);
    }

    long long narms;
    if ((RedisModule_StringToLongLong(argv[2],&narms) != REDISMODULE_OK)) {
        return RedisModule_ReplyWithError(ctx,"ERR invalid value: narms must be a signed 64 bit integer");
    }

    if (narms == 0) {
      return RedisModule_ReplyWithError(ctx,"ERR invalid value: narms must be > 0");
    }

    if (narms > MAX_ARMS) {
      return RedisModule_ReplyWithError(ctx,"ERR invalid value: too many arms");
    }

    double c;
    if ((RedisModule_StringToDouble(argv[3], &c) != REDISMODULE_OK)) {
      return RedisModule_ReplyWithError(ctx,"ERR invalid value: c must be a double");
    }

    /* Create an empty value object if the key is currently empty. */
    BanditUCBObject *hto;
    if (type == REDISMODULE_KEYTYPE_EMPTY) {
      hto = createBanditUCBObject(narms, c);
      RedisModule_ModuleTypeSetValue(key,BanditUCBType,hto);
    } else {
        hto = RedisModule_ModuleTypeGetValue(key);
    }

    zeroBanditUCBObject(hto);
    RedisModule_SignalKeyAsReady(ctx,argv[1]);

    RedisModule_ReplyWithLongLong(ctx, hto->narms);
    RedisModule_ReplicateVerbatim(ctx);
    return REDISMODULE_OK;
}


/* BANDITUCB.ADD <key> <arm> <reward>
 * Returns updated count and mean */
int BanditUCBAdd_RedisCommand(RedisModuleCtx *ctx, RedisModuleString **argv, int argc) {
  RedisModule_AutoMemory(ctx); /* Use automatic memory management. */

  if (argc != 4) return RedisModule_WrongArity(ctx);

  RedisModuleKey *key = RedisModule_OpenKey(ctx,argv[1],
					    REDISMODULE_READ|REDISMODULE_WRITE);


  int type = RedisModule_KeyType(key);
  if (type != REDISMODULE_KEYTYPE_EMPTY &&
      RedisModule_ModuleTypeGetType(key) != BanditUCBType)
    {
      return RedisModule_ReplyWithError(ctx,REDISMODULE_ERRORMSG_WRONGTYPE);
    }

  long long in_arm;
  double reward;

  if ((RedisModule_StringToLongLong(argv[2], &in_arm) != REDISMODULE_OK)) {
    return RedisModule_ReplyWithError(ctx,"ERR invalid value: must be a signed 64 bit integer");
  }

  if ((RedisModule_StringToDouble(argv[3],&reward) != REDISMODULE_OK)) {
    return RedisModule_ReplyWithError(ctx,"ERR invalid value: must be a double");
  }

  if (type == REDISMODULE_KEYTYPE_EMPTY) {
    return RedisModule_ReplyWithError(ctx, "ERR bandit needs to be initialized first");
  }

  BanditUCBObject *hto = RedisModule_ModuleTypeGetValue(key);

  if (in_arm < 0 || in_arm >= hto->narms) {
    return RedisModule_ReplyWithError(ctx, "ERR invalid arm");
  }

  const ARM arm = in_arm;

  const COUNT updated_count = hto->counts[arm] + 1;
  (hto->counts[arm])++;
  double updated_mean;
  if (updated_count == 1) {
    updated_mean = reward;
  } else {
    const double old_mean = hto->means[arm];
    updated_mean = old_mean + (reward - old_mean) / updated_count;
  }

  hto->means[arm] = updated_count;
  hto->means[arm] = updated_mean;

  RedisModule_SignalKeyAsReady(ctx,argv[1]);

  RedisModule_ReplyWithArray(ctx, 2);
  RedisModule_ReplyWithLongLong(ctx, hto->counts[arm]);
  RedisModule_ReplyWithDouble(ctx, hto->means[arm]);

  RedisModule_ReplicateVerbatim(ctx);
  return REDISMODULE_OK;
}


/* BANDITUCB.SET <key> <arm> <count> <mean>
 * Reply with count and mean */
int BanditUCBSet_RedisCommand(RedisModuleCtx *ctx, RedisModuleString **argv, int argc) {
  RedisModule_AutoMemory(ctx); /* Use automatic memory management. */

  if (argc != 5) return RedisModule_WrongArity(ctx);

  RedisModuleKey *key = RedisModule_OpenKey(ctx,argv[1],
					    REDISMODULE_READ|REDISMODULE_WRITE);
  int type = RedisModule_KeyType(key);
  if (type != REDISMODULE_KEYTYPE_EMPTY &&
      RedisModule_ModuleTypeGetType(key) != BanditUCBType)
    {
      return RedisModule_ReplyWithError(ctx,REDISMODULE_ERRORMSG_WRONGTYPE);
    }

  long long arm;
  long long count;
  double mean;

  if ((RedisModule_StringToLongLong(argv[2], &arm) != REDISMODULE_OK)) {
    return RedisModule_ReplyWithError(ctx,"ERR invalid value: arm must be an unsigned 64 bit integer");
  }

  if ((RedisModule_StringToLongLong(argv[3] ,&count) != REDISMODULE_OK)) {
    return RedisModule_ReplyWithError(ctx,"ERR invalid value: count must be an unsigned 64 bit integer");
  }

  if ((RedisModule_StringToDouble(argv[4], &mean) != REDISMODULE_OK)) {
    return RedisModule_ReplyWithError(ctx,"ERR invalid value: total must be a double");
  }

  if (type == REDISMODULE_KEYTYPE_EMPTY) {
    return RedisModule_ReplyWithError(ctx, "ERR bandit needs to be initialized first");
  }

  BanditUCBObject *hto = RedisModule_ModuleTypeGetValue(key);

  if (arm < 0 || arm >= hto->narms) {
    return RedisModule_ReplyWithError(ctx, "ERR invalid arm");
  }

  hto->counts[arm] = count;
  hto->means[arm] = mean;

  RedisModule_SignalKeyAsReady(ctx, argv[1]);
  
  RedisModule_ReplyWithArray(ctx, 2);
  RedisModule_ReplyWithLongLong(ctx, hto->counts[arm]);
  RedisModule_ReplyWithDouble(ctx, hto->means[arm]);

  RedisModule_ReplicateVerbatim(ctx);
  return REDISMODULE_OK;
}


/* draw from [0,n( evenly distributed by rejecting some of the range */
int randInt(int n) {  
  long limit = (RAND_MAX / n)*n;  
  int r;  
  while (true){ 
    r = rand(); 
    if(r < limit)
      break; 
  } 
  return r % n;  
} 


/* sum counts */
COUNT sumcounts(const COUNT *counts, uint64_t n) {
  uint64_t t = 0;
  for (ARM i = 0; i < n; ++i) {
    t += counts[i];
  }
  return t;
}


/* compute UCB bounds for all arms */
void computeBounds(BanditUCBObject *hto,
		   double* bounds) {
  const double t = sumcounts(hto->counts, hto->narms);
  const double logt = log(t);
  for(ARM i=0; i < hto->narms; ++i) {
    const double z = hto->c * sqrt(logt / hto->counts[i]);
    const double mean = hto->means[i];
    const double bound = mean + z;
    bounds[i] = bound;
  }
}


/* BANDITUCB.PICK <key>
 * Reply with the picked arm.
 * pick is non-deterministic (breaking ties) but that's OK as it doesn't change any state */
int BanditUCBPick_RedisCommand(RedisModuleCtx *ctx, RedisModuleString **argv, int argc) {
    RedisModule_AutoMemory(ctx); /* Use automatic memory management. */

    if (argc != 2) return RedisModule_WrongArity(ctx);
    RedisModuleKey *key = RedisModule_OpenKey(ctx,argv[1],
        REDISMODULE_READ|REDISMODULE_WRITE);
    int type = RedisModule_KeyType(key);
    if (type != REDISMODULE_KEYTYPE_EMPTY &&
        RedisModule_ModuleTypeGetType(key) != BanditUCBType)
    {
        return RedisModule_ReplyWithError(ctx,REDISMODULE_ERRORMSG_WRONGTYPE);
    }

    struct BanditUCBObject *hto = RedisModule_ModuleTypeGetValue(key);

    // single-threaded so OK
    static int choices[MAX_ARMS];
    static double bounds[MAX_ARMS];
    int* pchoices = choices;

    // if there are still unpulled arms pull one choose between them

    for(ARM i=0; i < hto->narms; ++i) {
      if (hto->counts[i] == 0) {
	*pchoices++ = i;
      }
    }

    int nchoices = pchoices - choices;
    if (nchoices == 0) {
      // all pulled at least once, compare UCB bounds

      computeBounds(hto, bounds);

      double bestBound = -INFINITY;
      for(ARM i=0; i < hto->narms; ++i) {
	if (bounds[i] > bestBound) {
	  bestBound = bounds[i];
	}
      }

      // it's floating point but ties are not necessarily zero probability
      // so consider all
      for (ARM i=0; i < hto->narms; ++i) {
	if (bounds[i] == bestBound) {
	  *pchoices++ = i;
	}
      }
      nchoices = pchoices - choices;
    }

    if (nchoices == 0) {
      return RedisModule_ReplyWithError(ctx,"no choices");
    }

    // pick from choices
    int ichoice;
    if (nchoices == 1) {
      // only 1 option, no need to draw at random
      ichoice = 0;
    } else {
      ichoice = randInt(nchoices);
    }
    ARM arm = choices[ichoice];

    RedisModule_ReplyWithLongLong(ctx, arm);
    
    return REDISMODULE_OK;
}


/* BANDITUCB.COUNTS <key>
 * Reply with counts for all arms
 */
int BanditUCBCounts_RedisCommand(RedisModuleCtx *ctx, RedisModuleString **argv, int argc) {
    RedisModule_AutoMemory(ctx); /* Use automatic memory management. */

    if (argc != 2) return RedisModule_WrongArity(ctx);
    RedisModuleKey *key = RedisModule_OpenKey(ctx,argv[1],
        REDISMODULE_READ|REDISMODULE_WRITE);
    int type = RedisModule_KeyType(key);
    if (type != REDISMODULE_KEYTYPE_EMPTY &&
        RedisModule_ModuleTypeGetType(key) != BanditUCBType)
    {
        return RedisModule_ReplyWithError(ctx,REDISMODULE_ERRORMSG_WRONGTYPE);
    }

    BanditUCBObject *hto = RedisModule_ModuleTypeGetValue(key);

    if (type == REDISMODULE_KEYTYPE_EMPTY) {
          return RedisModule_ReplyWithError(ctx, "ERR bandit needs to be initialized first");
    }

    RedisModule_ReplyWithArray(ctx,hto->narms);
    for (ARM i = 0; i < hto->narms; ++i) {
        RedisModule_ReplyWithLongLong(ctx, hto->counts[i]);
    }

    return REDISMODULE_OK;
}


/* BANDITUCB.MEANS <key>
 * Reply with means for all arms
 */
int BanditUCBMeans_RedisCommand(RedisModuleCtx *ctx, RedisModuleString **argv, int argc) {
    RedisModule_AutoMemory(ctx); /* Use automatic memory management. */

    if (argc != 2) return RedisModule_WrongArity(ctx);
    RedisModuleKey *key = RedisModule_OpenKey(ctx,argv[1],
        REDISMODULE_READ|REDISMODULE_WRITE);
    int type = RedisModule_KeyType(key);
    if (type != REDISMODULE_KEYTYPE_EMPTY &&
        RedisModule_ModuleTypeGetType(key) != BanditUCBType)
    {
        return RedisModule_ReplyWithError(ctx,REDISMODULE_ERRORMSG_WRONGTYPE);
    }

    struct BanditUCBObject *hto = RedisModule_ModuleTypeGetValue(key);

    if (type == REDISMODULE_KEYTYPE_EMPTY) {
          return RedisModule_ReplyWithError(ctx, "ERR bandit needs to be initialized first");
    }

    RedisModule_ReplyWithArray(ctx,hto->narms);
    for (ARM i = 0; i < hto->narms; ++i) {
        RedisModule_ReplyWithDouble(ctx, hto->means[i]);
    }

    return REDISMODULE_OK;
}


/* BANDITUCB.BOUNDS <key>
 * Reply with UCB bounds for all arms
 */
int BanditUCBBounds_RedisCommand(RedisModuleCtx *ctx, RedisModuleString **argv, int argc) {
    RedisModule_AutoMemory(ctx); /* Use automatic memory management. */

    if (argc != 2) return RedisModule_WrongArity(ctx);
    RedisModuleKey *key = RedisModule_OpenKey(ctx,argv[1],
					      REDISMODULE_READ|REDISMODULE_WRITE);
    int type = RedisModule_KeyType(key);
    if (type != REDISMODULE_KEYTYPE_EMPTY &&
        RedisModule_ModuleTypeGetType(key) != BanditUCBType) {
      return RedisModule_ReplyWithError(ctx,REDISMODULE_ERRORMSG_WRONGTYPE);
    }
    
    BanditUCBObject *hto = RedisModule_ModuleTypeGetValue(key);

    // single-threaded so OK
    static double bounds[MAX_ARMS];
    computeBounds(hto, bounds);
    
    RedisModule_ReplyWithArray(ctx,hto->narms);

    for (ARM i = 0; i < hto->narms; ++i) {
      RedisModule_ReplyWithDouble(ctx, bounds[i]);
    }

    return REDISMODULE_OK;
}


/* Load BanditUCBObject from RDB */
void *BanditUCBRdbLoad(RedisModuleIO *rdb, int encver) {

    if (encver != 0) {
        return NULL;
    }

    ARM narms = RedisModule_LoadUnsigned(rdb);
    double c = RedisModule_LoadDouble(rdb);
    BanditUCBObject *hto = createBanditUCBObject(narms, c);
    for(ARM i=0; i < hto->narms; ++i) {
      hto->counts[i] = RedisModule_LoadUnsigned(rdb);
    }
    for(ARM i=0; i < hto->narms; ++i) {
      hto->means[i] = RedisModule_LoadDouble(rdb);
    }
    
    return hto;
}


/* Save BanditUCB object to RDB */
void BanditUCBRdbSave(RedisModuleIO *rdb, void *value) {

    BanditUCBObject *hto = value;
    RedisModule_SaveUnsigned(rdb, hto->narms);
    RedisModule_SaveDouble(rdb, hto->c);
    for (ARM i = 0; i < hto->narms; ++i) {
      RedisModule_SaveUnsigned(rdb, hto->counts[i]);
    }
    for (ARM i = 0; i < hto->narms; ++i) {
      RedisModule_SaveDouble(rdb, hto->means[i]);
    }
}


/* Rewrite BanditUCB object in AOF
 * As a single BANDITUCB.SET command for each arm */
void BanditUCBAofRewrite(RedisModuleIO *aof, RedisModuleString *key, void *value) {

  BanditUCBObject *hto = value;
  RedisModule_EmitAOF(aof, "BANDITUCB.INIT", "sld", key, hto->narms, hto->c);
  for(ARM i = 0; i < hto->narms; ++i) {
    RedisModule_EmitAOF(aof, "BANDITUCB.SET", "slld", key, i, hto->counts[i], hto->means[i]);
  }
}


/* Compute memory usage */
size_t BanditUCBMemUsage(const void *value) {
    const BanditUCBObject *hto = value;
    return hto->narms * (sizeof(COUNT) + sizeof(double)) + sizeof(*hto);
}


/* Free BanditUCBObject */
void BanditUCBFree(void *value) {
    BanditUCBReleaseObject(value);
}


/* Conpute digest for BanditUCBObject */
void BanditUCBDigest(RedisModuleDigest *md, void *value) {

    BanditUCBObject *hto = value;
    RedisModule_DigestAddLongLong(md,hto->narms);
    for(ARM i = 0; i < hto->narms; ++i) {
        RedisModule_DigestAddLongLong(md, hto->counts[i]);
    }
    for(ARM i = 0; i < hto->narms; ++i) {
      // there is no DigestAddDouble. casting to long long, fine for digest
      RedisModule_DigestAddLongLong(md, (long long)hto->means[i]);
    }
    RedisModule_DigestEndSequence(md);
}


/* Register and setup everything on load */
int RedisModule_OnLoad(RedisModuleCtx *ctx, RedisModuleString **argv, int argc) {
    REDISMODULE_NOT_USED(argv);
    REDISMODULE_NOT_USED(argc);

    if (RedisModule_Init(ctx,"banditucb",1,REDISMODULE_APIVER_1)
        == REDISMODULE_ERR) return REDISMODULE_ERR;

    RedisModuleTypeMethods tm = {
        .version = REDISMODULE_TYPE_METHOD_VERSION,
        .rdb_load = BanditUCBRdbLoad,
        .rdb_save = BanditUCBRdbSave,
        .aof_rewrite = BanditUCBAofRewrite,
        .mem_usage = BanditUCBMemUsage,
        .free = BanditUCBFree,
        .digest = BanditUCBDigest
    };

    BanditUCBType = RedisModule_CreateDataType(ctx,"banditucb",0,&tm);
    if (BanditUCBType == NULL) return REDISMODULE_ERR;

    if (RedisModule_CreateCommand(ctx,"banditucb.init",
        BanditUCBInit_RedisCommand,"write deny-oom",1,1,1) == REDISMODULE_ERR)
        return REDISMODULE_ERR;

    if (RedisModule_CreateCommand(ctx,"banditucb.add",
        BanditUCBAdd_RedisCommand,"write deny-oom",1,1,1) == REDISMODULE_ERR)
        return REDISMODULE_ERR;

    if (RedisModule_CreateCommand(ctx,"banditucb.set",
        BanditUCBSet_RedisCommand,"write deny-oom",1,1,1) == REDISMODULE_ERR)
        return REDISMODULE_ERR;
    
    if (RedisModule_CreateCommand(ctx,"banditucb.pick",
        BanditUCBPick_RedisCommand,"readonly",1,1,1) == REDISMODULE_ERR)
        return REDISMODULE_ERR;

    if (RedisModule_CreateCommand(ctx,"banditucb.counts",
        BanditUCBCounts_RedisCommand,"readonly",1,1,1) == REDISMODULE_ERR)
        return REDISMODULE_ERR;

    if (RedisModule_CreateCommand(ctx,"banditucb.means",
        BanditUCBMeans_RedisCommand,"readonly",1,1,1) == REDISMODULE_ERR)
        return REDISMODULE_ERR;

    if (RedisModule_CreateCommand(ctx,"banditucb.bounds",
        BanditUCBBounds_RedisCommand,"readonly",1,1,1) == REDISMODULE_ERR)
        return REDISMODULE_ERR;
    
    return REDISMODULE_OK;
}

