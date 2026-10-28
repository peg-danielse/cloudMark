#define _POSIX_C_SOURCE 200809L

#include <errno.h>
#include <limits.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/stat.h>
#include <unistd.h>

static const char *kServer = "/usr/bin/redis-server";
static char kRoot[PATH_MAX];
static const int kDefaultPort = 6379;

static void init_paths(void) {
  char path[PATH_MAX];
  ssize_t n = readlink("/proc/self/exe", path, sizeof path - 1);
  if (n < 0) {
    perror("redis-server: readlink");
    exit(1);
  }
  path[n] = '\0';
  char *slash = strrchr(path, '/');
  if (slash != NULL) {
    *slash = '\0';
  }
  snprintf(kRoot, sizeof kRoot, "%s", path);
}

static int parse_port(const char *text) {
  char *end = NULL;
  errno = 0;
  long value = strtol(text, &end, 10);
  if (text[0] == '\0' || *end != '\0' || errno != 0 || value < 1 || value > 65535) {
    fprintf(stderr, "redis-server: invalid --advertise-port %s\n", text);
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
        fprintf(stderr, "redis-server: --advertise-port needs a value\n");
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
    fprintf(stderr, "redis-server: unknown argument %s\n", arg);
    exit(2);
  }
  return port;
}

static void mkdir_one(const char *path) {
  if (mkdir(path, 0755) == 0 || errno == EEXIST) {
    return;
  }
  fprintf(stderr, "redis-server: mkdir %s: %s\n", path, strerror(errno));
  exit(1);
}

int main(int argc, char **argv) {
  init_paths();
  int port = advertise_port(argc, argv);
  char datadir[PATH_MAX];
  char port_text[16];
  char *argv_exec[] = {
      "redis-server",
      "--bind", "127.0.0.1",
      "--port", port_text,
      "--dir", datadir,
      "--dbfilename", "dump.rdb",
      "--daemonize", "no",
      "--protected-mode", "yes",
      "--logfile", "",
      "--save", "",          /* no RDB snapshots: every start is empty (DSB runs need equal state) */
      "--appendonly", "no",
      NULL};

  snprintf(port_text, sizeof port_text, "%d", port);
  snprintf(datadir, sizeof datadir, "%s/%d", kRoot, port);

  mkdir_one(kRoot);
  mkdir_one(datadir);

  execv(kServer, argv_exec);
  perror(kServer);
  return 1;
}
