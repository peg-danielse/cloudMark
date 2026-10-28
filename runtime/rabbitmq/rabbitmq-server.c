#define _POSIX_C_SOURCE 200809L

#include <errno.h>
#include <limits.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/stat.h>
#include <unistd.h>

static const char *kServer = "/usr/lib/rabbitmq/bin/rabbitmq-server";
static char kRoot[PATH_MAX];
static const int kDefaultPort = 5672;

static void init_paths(void) {
  char path[PATH_MAX];
  ssize_t n = readlink("/proc/self/exe", path, sizeof path - 1);
  if (n < 0) {
    perror("rabbitmq-server: readlink");
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
    fprintf(stderr, "rabbitmq-server: invalid --advertise-port %s\n", text);
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
        fprintf(stderr, "rabbitmq-server: --advertise-port needs a value\n");
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
    fprintf(stderr, "rabbitmq-server: unknown argument %s\n", arg);
    exit(2);
  }
  return port;
}

static void mkdir_one(const char *path) {
  if (mkdir(path, 0755) == 0 || errno == EEXIST) {
    return;
  }
  fprintf(stderr, "rabbitmq-server: mkdir %s: %s\n", path, strerror(errno));
  exit(1);
}

static void write_config(const char *path, int port) {
  FILE *file = fopen(path, "w");
  if (file == NULL) {
    fprintf(stderr, "rabbitmq-server: open %s: %s\n", path, strerror(errno));
    exit(1);
  }
  if (fprintf(file, "listeners.tcp.default = %d\n", port) < 0) {
    fprintf(stderr, "rabbitmq-server: write %s: %s\n", path, strerror(errno));
    fclose(file);
    exit(1);
  }
  if (fclose(file) != 0) {
    fprintf(stderr, "rabbitmq-server: close %s: %s\n", path, strerror(errno));
    exit(1);
  }
}

int main(int argc, char **argv) {
  init_paths();
  int port = advertise_port(argc, argv);
  char base[PATH_MAX];
  char mnesia[PATH_MAX];
  char logs[PATH_MAX];
  char config[PATH_MAX];
  char port_text[16];
  char nodename[256];
  char hostname[256];
  char *argv_exec[] = {"rabbitmq-server", NULL};

  snprintf(port_text, sizeof port_text, "%d", port);
  snprintf(base, sizeof base, "%s/%d", kRoot, port);
  snprintf(mnesia, sizeof mnesia, "%s/mnesia", base);
  snprintf(logs, sizeof logs, "%s/log", base);
  snprintf(config, sizeof config, "%s/rabbitmq.conf", base);

  mkdir_one(kRoot);
  mkdir_one(base);
  mkdir_one(mnesia);
  mkdir_one(logs);
  write_config(config, port);

  if (gethostname(hostname, sizeof hostname) != 0) {
    perror("gethostname");
    return 1;
  }
  hostname[sizeof hostname - 1] = '\0';
  char *dot = strchr(hostname, '.');
  if (dot != NULL) {
    *dot = '\0';
  }
  snprintf(nodename, sizeof nodename, "rabbit-%d@%s", port, hostname);

  if (setenv("RABBITMQ_NODE_PORT", port_text, 1) != 0 ||
      setenv("RABBITMQ_NODENAME", nodename, 1) != 0 ||
      setenv("RABBITMQ_MNESIA_BASE", mnesia, 1) != 0 ||
      setenv("RABBITMQ_LOG_BASE", logs, 1) != 0 ||
      setenv("RABBITMQ_CONFIG_FILE", config, 1) != 0 ||
      setenv("HOME", base, 1) != 0) {
    perror("setenv");
    return 1;
  }

  execv(kServer, argv_exec);
  perror(kServer);
  return 1;
}
