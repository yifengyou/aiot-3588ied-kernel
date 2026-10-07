#include <stdio.h>
#include <stdint.h>
#include <stdlib.h>
#include <stdarg.h>
#include <err.h>
#include <errno.h>

#include <linux/types.h>
#include <unistd.h>
#include <linux/i2c.h>
#include <linux/i2c-dev.h>

#include <sys/ioctl.h>
#include <sys/types.h>
#include <sys/stat.h>
#include <fcntl.h>
#include <string.h>
#include <time.h>

#define I2C_DEV "/dev/i2c-6"
#define MCU_I2C_ADDR 0x62
#define LOG_FILE "/var/log/smdt_wdt.log"
#define FEED_INTERVAL 20

#define WIN_1MIN  (60)
#define WIN_15MIN (15 * 60)
#define WIN_30MIN (30 * 60)
#define MAX_SLOTS (WIN_30MIN / FEED_INTERVAL + 2)

int fd = -1;
static FILE *logf = NULL;

static struct {
	time_t ts;
	int ok;
} ring[MAX_SLOTS];
static int ring_head = 0;
static int ring_count = 0;

static void ring_push(time_t ts, int ok)
{
	ring[ring_head].ts = ts;
	ring[ring_head].ok = ok;
	ring_head = (ring_head + 1) % MAX_SLOTS;
	if (ring_count < MAX_SLOTS)
		ring_count++;
}

static int ring_query(time_t since, int *total, int *ok_count, int *fail_count)
{
	*total = 0;
	*ok_count = 0;
	*fail_count = 0;
	int found = 0;
	for (int i = 0; i < ring_count; i++) {
		int idx = (ring_head - ring_count + i + MAX_SLOTS) % MAX_SLOTS;
		if (ring[idx].ts >= since) {
			found = 1;
			(*total)++;
			if (ring[idx].ok)
				(*ok_count)++;
			else
				(*fail_count)++;
		}
	}
	return found;
}

static void log_write(const char *fmt, ...)
{
	char buf[512];
	va_list ap;
	va_start(ap, fmt);
	vsnprintf(buf, sizeof(buf), fmt, ap);
	va_end(ap);

	time_t now = time(NULL);
	struct tm *tm = localtime(&now);

	fprintf(logf, "[%04d-%02d-%02d %02d:%02d:%02d] %s\n",
		tm->tm_year + 1900, tm->tm_mon + 1, tm->tm_mday,
		tm->tm_hour, tm->tm_min, tm->tm_sec, buf);
	fflush(logf);
}

static void log_summary(time_t now)
{
	int total, ok, fail;
	char line[256];

	log_write("=== Watchdog Summary ===");

	if (ring_query(now - WIN_1MIN, &total, &ok, &fail))
		log_write("Last 1 min:  total=%d ok=%d fail=%d", total, ok, fail);
	else
		log_write("Last 1 min:  no data");

	if (ring_query(now - WIN_15MIN, &total, &ok, &fail))
		log_write("Last 15 min: total=%d ok=%d fail=%d", total, ok, fail);
	else
		log_write("Last 15 min: no data");

	if (ring_query(now - WIN_30MIN, &total, &ok, &fail))
		log_write("Last 30 min: total=%d ok=%d fail=%d", total, ok, fail);
	else
		log_write("Last 30 min: no data");
}

static int i2c_write(uint8_t reg, uint8_t val, int ms)
{
	int retries;
	uint8_t data[2];

	data[0] = reg;
	data[1] = val;

	for (retries = 5; retries; retries--) {
		if (write(fd, data, sizeof(data)) == sizeof(data)) {
			return 0;
		}
		usleep(1000 * 10);
	}
	if (ms) {
		usleep(ms * 1000);
	}

	return -1;
}

static int i2c_read(uint8_t reg, uint8_t *val, int ms)
{
	int retries;

	for (retries = 5; retries; retries--) {
		if (write(fd, &reg, 1) != 1) {
			return -1;
		}
		if (read(fd, val, 1) != 1) {
			return -1;
		}
	}
	if (ms) {
		usleep(ms * 1000);
	}

	return 0;
}

static int i2c_check_val(uint8_t reg, uint8_t val, int ms)
{
	uint8_t t = 0;
	if (i2c_read(reg, &t, ms) < 0) {
		printf("Error: read Reg[0x%x] failed\n", reg);
		return -1;
	}
	if (t != val) {
		printf("Error: expect Reg[0x%x] is 0x%x, in fact 0x%x\n", reg, val, t);
		return -1;
	}
	return 0;
}

static int wdt_simulator()
{
	int retries;
	i2c_check_val(0x3a, 0x89, 0);

	for (retries == 2; retries; retries--) {
		i2c_check_val(0xed, 0x00, 5);
		i2c_check_val(0xeb, 0x8c, 5);
		i2c_check_val(0xea, 0xf5, 5);
		i2c_check_val(0xe9, 0x68, 5);
		i2c_check_val(0xe8, 0x5e, 5);
	}

	i2c_check_val(0x3b, 0xb1, 16);
	i2c_check_val(0x3b, 0xb1, 0);
	i2c_write(0x32, 0x01, 9);
	i2c_check_val(0xb2, 0x01, 12000);
	i2c_write(0x51, 0x33, 8);
	i2c_check_val(0xd1, 0x33, 1900);
	i2c_check_val(0xb1, 0x00, 5000);
}

static int wdt_simulator_lite()
{
	i2c_write(0x32, 0x01, 9);
	i2c_write(0x51, 0x33, 8);
}

static int wdt_init(void)
{
	fd = open(I2C_DEV, O_RDWR);

	if (fd < 0) {
		perror("Can't open " I2C_DEV " \n");
		return -1;
	}
	printf("Open " I2C_DEV " success !\n");
	if (ioctl(fd, I2C_SLAVE, MCU_I2C_ADDR) < 0) {
		perror("Failed to set i2c device slave address!\n");
		close(fd);
		return -1;
	}

	return 0;
}

static int wdt_enable()
{
	return i2c_check_val(0xb2, 0x00, 0);
}

static int wdt_disable()
{
	return i2c_write(0x32, 0x00, 0);
}

static int wdt_prepare()
{
	wdt_disable();
	if (i2c_write(0x32, 0x01, 9) < 0) {
		log_write("ERROR: wdt_prepare enable failed");
		return -1;
	}
	if (i2c_write(0x51, 0x33, 8) < 0) {
		log_write("ERROR: wdt_prepare set timeout failed");
		return -1;
	}
	log_write("watchdog prepared (i2c6 @ 0x62)");
	return 0;
}

static int wdt_feed()
{
	return i2c_write(0x33, 0xab, 0);
}

int main()
{
	int count = 0;
	time_t last_summary = 0;

	logf = fopen(LOG_FILE, "w");
	if (!logf) {
		fprintf(stderr, "Cannot open %s: %s\n", LOG_FILE, strerror(errno));
		return -1;
	}

	if (wdt_init() < 0) {
		log_write("ERROR: cannot open %s", I2C_DEV);
		return -1;
	}
	log_write("Open %s success", I2C_DEV);
	wdt_prepare();

	while (1) {
		time_t now = time(NULL);
		int ok = (wdt_feed() >= 0);
		count++;
		ring_push(now, ok);

		if (ok)
			log_write("feed OK total=%d", count);
		else
			log_write("feed FAILED total=%d", count);

		if (now - last_summary >= 60) {
			log_summary(now);
			last_summary = now;
		}

		sleep(FEED_INTERVAL);
	}

	fclose(logf);
	return 0;
}
