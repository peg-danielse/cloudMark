#define _POSIX_C_SOURCE 200809L

#include <errno.h>
#include <limits.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/stat.h>
#include <unistd.h>

static char kServer[PATH_MAX];
static char kRoot[PATH_MAX];
static const int kDefaultPort = 27017;

static void init_paths(void) {
  char path[PATH_MAX];
  ssize_t n = readlink("/proc/self/exe", path, sizeof path - 1);
  if (n < 0) {
    perror("mongod: readlink");
    exit(1);
  }
  path[n] = '\0';
  char *slash = strrchr(path, '/');
  if (slash != NULL) {
    *slash = '\0';
  }
  snprintf(kRoot, sizeof kRoot, "%s", path);
  snprintf(kServer, sizeof kServer, "%s/bin/mongod", kRoot);
}

static int parse_port(const char *text) {
  char *end = NULL;
  errno = 0;
  long value = strtol(text, &end, 10);
  if (text[0] == '\0' || *end != '\0' || errno != 0 || value < 1 || value > 65535) {
    fprintf(stderr, "mongod: invalid --advertise-port %s\n", text);
    exit(2);
  }
  return (int)value;
}

static int advertise_port(int argc, char **argv) {
  int port = kDefaultPort;
  for (int i = 1; i < argc; i++) {
    const char *arg = argv[i];
    if (strcmp(arg, "--advertise-port") == 0) {
      if (i + 1 >= argc) {
        fprintf(stderr, "mongod: --advertise-port needs a value\n");
        exit(2);
      }
      port = parse_port(argv[++i]);
      continue;
    }
    const char *prefix = "--advertise-port=";
    size_t prefix_len = strlen(prefix);
    if (strncmp(arg, prefix, prefix_len) == 0) {
      port = parse_port(arg + prefix_len);
      continue;
    }
    fprintf(stderr, "mongod: unknown argument %s\n", arg);
    exit(2);
  }
  return port;
}

static void mkdir_one(const char *path) {
  if (mkdir(path, 0755) == 0 || errno == EEXIST) {
    return;
  }
  fprintf(stderr, "mongod: mkdir %s: %s\n", path, strerror(errno));
  exit(1);
}

int main(int argc, char **argv) {
  init_paths();
  int port = advertise_port(argc, argv);
  char base[PATH_MAX];
  char dbpath[PATH_MAX];
  char port_text[16];
  char read_arg[80];
  char write_arg[80];
  char *argv_exec[16];
  int n = 0;

  snprintf(port_text, sizeof port_text, "%d", port);
  snprintf(base, sizeof base, "%s/%d", kRoot, port);
  snprintf(dbpath, sizeof dbpath, "%s/db", base);

  argv_exec[n++] = "mongod";
  argv_exec[n++] = "--dbpath";
  argv_exec[n++] = dbpath;
  argv_exec[n++] = "--port";
  argv_exec[n++] = port_text;
  argv_exec[n++] = "--bind_ip";
  argv_exec[n++] = "127.0.0.1";
  argv_exec[n++] = "--nounixsocket";
  // These come from Mercury's /start env. Absent keeps mongod's own defaults.
  const char *reads = getenv("MONGO_WT_READ_TRANSACTIONS");
  const char *writes = getenv("MONGO_WT_WRITE_TRANSACTIONS");
  if (reads != NULL && reads[0] != '\0') {
    snprintf(read_arg, sizeof read_arg, "wiredTigerConcurrentReadTransactions=%s", reads);
    argv_exec[n++] = "--setParameter";
    argv_exec[n++] = read_arg;
  }
  if (writes != NULL && writes[0] != '\0') {
    snprintf(write_arg, sizeof write_arg, "wiredTigerConcurrentWriteTransactions=%s", writes);
    argv_exec[n++] = "--setParameter";
    argv_exec[n++] = write_arg;
  }
  argv_exec[n] = NULL;

  mkdir_one(kRoot);
  mkdir_one(base);
  mkdir_one(dbpath);

  execv(kServer, argv_exec);
  perror(kServer);
  return 1;
}
