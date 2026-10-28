#define _POSIX_C_SOURCE 200809L

#include <errno.h>
#include <libgen.h>
#include <limits.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <unistd.h>

static char kServer[PATH_MAX];

static void init_paths(void) {
  char path[PATH_MAX];
  ssize_t n = readlink("/proc/self/exe", path, sizeof path - 1);
  if (n < 0) {
    perror("memcached: readlink");
    exit(1);
  }
  path[n] = '\0';
  char *slash = strrchr(path, '/');
  if (slash != NULL) {
    *slash = '\0';
  }
  snprintf(kServer, sizeof kServer, "%s/local/bin/memcached", path);
}

static int parse_port(const char *text) {
  char *end = NULL;
  errno = 0;
  long value = strtol(text, &end, 10);
  if (text[0] == '\0' || *end != '\0' || errno != 0 || value < 1 || value > 65535) {
    return -1;
  }
  return (int)value;
}

static int port_from_name(const char *argv0) {
  char path[PATH_MAX];
  snprintf(path, sizeof path, "%s", argv0);
  const char *name = basename(path);
  return parse_port(name);
}

static int advertise_port(int argc, char **argv) {
  int port = port_from_name(argv[0]);
  for (int i = 1; i < argc; i++) {
    const char *arg = argv[i];
    if (strcmp(arg, "--advertise-port") == 0) {
      if (i + 1 >= argc) {
        fprintf(stderr, "memcached: --advertise-port needs a value\n");
        exit(2);
      }
      port = parse_port(argv[++i]);
      if (port < 0) {
        fprintf(stderr, "memcached: invalid --advertise-port %s\n", argv[i]);
        exit(2);
      }
      continue;
    }
    const char *prefix = "--advertise-port=";
    size_t prefix_len = strlen(prefix);
    if (strncmp(arg, prefix, prefix_len) == 0) {
      port = parse_port(arg + prefix_len);
      if (port < 0) {
        fprintf(stderr, "memcached: invalid --advertise-port %s\n", arg + prefix_len);
        exit(2);
      }
      continue;
    }
    fprintf(stderr, "memcached: unknown argument %s\n", arg);
    exit(2);
  }
  if (port < 0) {
    fprintf(stderr, "memcached: pass --advertise-port or name the executable with the port\n");
    exit(2);
  }
  return port;
}

int main(int argc, char **argv) {
  init_paths();
  int port = advertise_port(argc, argv);
  char port_text[16];
  snprintf(port_text, sizeof port_text, "%d", port);

  // MEMCACHED_THREADS comes from Mercury's /start env. Absent keeps memcached's own default.
  const char *threads = getenv("MEMCACHED_THREADS");
  // 1.6 rejects -c 0 ("must be greater than 0"), so there is no unlimited setting.
  // 8192 covers a 2048-user step plus the idle pool, and the listen backlog has
  // to match or the extra dials are refused before they become connections.
  char *argv_exec[20];
  int n = 0;
  argv_exec[n++] = "memcached";
  argv_exec[n++] = "-l";
  argv_exec[n++] = "127.0.0.1";
  argv_exec[n++] = "-p";
  argv_exec[n++] = port_text;
  argv_exec[n++] = "-U";
  argv_exec[n++] = "0";
  argv_exec[n++] = "-m";
  argv_exec[n++] = "64";
  argv_exec[n++] = "-c";
  argv_exec[n++] = "8192";
  argv_exec[n++] = "-b";
  argv_exec[n++] = "8192";
  if (threads != NULL && threads[0] != '\0') {
    argv_exec[n++] = "-t";
    argv_exec[n++] = (char *)threads;
  }
  // Recent memcached refuses to run as root unless -u names that user.
  if (geteuid() == 0) {
    argv_exec[n++] = "-u";
    argv_exec[n++] = "root";
  }
  argv_exec[n] = NULL;
  execv(kServer, argv_exec);
  perror(kServer);
  return 1;
}
