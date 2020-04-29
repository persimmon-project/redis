#include "server.h"

#include <libpsm/psm.h>

struct client *createFakeClient(void);
void freeFakeClientArgv(struct client *c);

void logToPSM(struct redisCommand *cmd, robj **argv, int _argc) {
    int8_t argc = _argc;
    int total_len = sizeof(argc) + sizeof(cmd);
    struct {
        sds s;
        size_t len;
    } argvs[argc];

    for (int i = 0; i < argc; i++) {
        robj *o = getDecodedObject(argv[i]);
        sds s = o->ptr;
        size_t len = sdslen(s) + 1;
        argvs[i].s = s;
        argvs[i].len = len;
        total_len += len;
        /* Although o is still in use, there's no opportunity for deallocation, and so it's fine (?) to decrement its refcount here. */
        decrRefCount(o);
    }

    void *start = psm_reserve(total_len);
    char *p = (char *) start;
    memcpy(p, &argc, sizeof(argc));
    p += sizeof(argc);
    memcpy(p, &cmd, sizeof(cmd));
    p += sizeof(cmd);
    for (int i = 0; i < argc; i++) {
        size_t len = argvs[i].len;
        memcpy(p, argvs[i].s, len);
        p += len;
    }
}

static int consumeLogEntry(const void *buf) {
    static client *fakeClient; // Fake client to execute logged commands with.
    if (fakeClient == NULL) {
        fakeClient = createFakeClient();
    }

    const char *p = buf;

    int8_t argc;
    memcpy(&argc, p, sizeof(argc));
    p += sizeof(int8_t);

    struct redisCommand *cmd;
    memcpy(&cmd, p, sizeof(cmd));
    p += sizeof(struct redisCommand *);

    robj **argv = zmalloc(sizeof(robj*)*argc);
    for (int i = 0; i < argc; i++) {
        const char *end = rawmemchr(p, '\0');
        argv[i] = createRawStringObject(p, end - p);
        p = end + 1;
    }

    fakeClient->argc = argc;
    fakeClient->argv = argv;
    fakeClient->cmd = cmd;
    cmd->proc(fakeClient);

    /* Clean up. Command code may have changed argv/argc so we use the
        * argv/argc of the client instead of the local variables. */
    freeFakeClientArgv(fakeClient);
    fakeClient->cmd = NULL;

    return p - (const char *) buf;
}

void initializePSM(void) {
    serverAssert(server.psm_mode != PSM_DISABLED);

    psm_config_t config;
    config.pin_core = -1;
    config.pmem_path = "/mnt/pmem1/redis"; // FIXME(zhangwen): hard code bad!
    config.use_sga = false;
    config.consume_func = consumeLogEntry;

    switch (server.psm_mode) {
    case PSM_NO_PERSIST:
        config.mode = PSM_MODE_NO_PERSIST;
        break;
    case PSM_UNDO:
        config.mode = PSM_MODE_UNDO;
        config.undo.criu_service_path = "/tmp/criu_service.socket";
        break;
    default:
        serverAssert(false);
    }

    if (0 != psm_init(&config)) {
        serverLog(LL_WARNING, "Failed to initialize PSM.");
        exit(1);
    }
}

void commitPSM(void) {
    psm_commit();
}
