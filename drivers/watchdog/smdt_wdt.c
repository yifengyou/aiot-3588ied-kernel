// SPDX-License-Identifier: GPL-2.0+
/*
 * SMDT MCU I2C Watchdog Driver
 *
 * External SMDT MCU watchdog connected via I2C.
 * Feed protocol: write 0xab to register 0x33 every ~20 seconds.
 * Init: disable (reg 0x32 = 0x00), enable (reg 0x32 = 0x01),
 *       set timeout (reg 0x51 = 0x33).
 *
 * Copyright (C) 2024 AIoT-3588IED
 */

#include <linux/bitops.h>
#include <linux/delay.h>
#include <linux/i2c.h>
#include <linux/kernel.h>
#include <linux/module.h>
#include <linux/of.h>
#include <linux/platform_device.h>
#include <linux/property.h>
#include <linux/watchdog.h>

#define SMDT_WDT_REG_ENABLE	0x32
#define SMDT_WDT_REG_TIMEOUT	0x51
#define SMDT_WDT_REG_FEED	0x33

#define SMDT_WDT_ENABLE_VAL	0x01
#define SMDT_WDT_DISABLE_VAL	0x00
#define SMDT_WDT_TIMEOUT_VAL	0x33
#define SMDT_WDT_FEED_VAL	0xab

#define SMDT_WDT_DEFAULT_TIMEOUT	60
#define SMDT_WDT_MIN_TIMEOUT		10
#define SMDT_WDT_MAX_TIMEOUT		120
#define SMDT_WDT_FEED_INTERVAL		20

struct smdt_wdt {
	struct i2c_client *client;
	struct watchdog_device wdd;
	struct delayed_work feed_work;
};

static int smdt_wdt_i2c_write(struct i2c_client *client, u8 reg, u8 val)
{
	struct i2c_msg msg;
	u8 buf[2];
	int ret;

	buf[0] = reg;
	buf[1] = val;

	msg.addr = client->addr;
	msg.flags = client->flags & I2C_M_TEN;
	msg.len = 2;
	msg.buf = buf;

	ret = i2c_transfer(client->adapter, &msg, 1);
	if (ret != 1) {
		dev_err(&client->dev, "i2c write reg 0x%02x failed: %d\n", reg, ret);
		return ret < 0 ? ret : -EIO;
	}

	return 0;
}

static int smdt_wdt_start(struct watchdog_device *wdd)
{
	struct smdt_wdt *wdt = watchdog_get_drvdata(wdd);
	int ret;

	ret = smdt_wdt_i2c_write(wdt->client, SMDT_WDT_REG_ENABLE,
				 SMDT_WDT_ENABLE_VAL);
	if (ret)
		return ret;

	usleep_range(9000, 10000);

	ret = smdt_wdt_i2c_write(wdt->client, SMDT_WDT_REG_TIMEOUT,
				 SMDT_WDT_TIMEOUT_VAL);
	if (ret)
		return ret;

	usleep_range(8000, 9000);

	schedule_delayed_work(&wdt->feed_work,
			      SMDT_WDT_FEED_INTERVAL * HZ);

	return 0;
}

static int smdt_wdt_stop(struct watchdog_device *wdd)
{
	struct smdt_wdt *wdt = watchdog_get_drvdata(wdd);

	cancel_delayed_work_sync(&wdt->feed_work);

	return smdt_wdt_i2c_write(wdt->client, SMDT_WDT_REG_ENABLE,
				  SMDT_WDT_DISABLE_VAL);
}

static int smdt_wdt_ping(struct watchdog_device *wdd)
{
	struct smdt_wdt *wdt = watchdog_get_drvdata(wdd);

	return smdt_wdt_i2c_write(wdt->client, SMDT_WDT_REG_FEED,
				  SMDT_WDT_FEED_VAL);
}

static void smdt_wdt_feed_work(struct work_struct *work)
{
	struct smdt_wdt *wdt = container_of(work, struct smdt_wdt,
					     feed_work.work);

	smdt_wdt_ping(&wdt->wdd);

	schedule_delayed_work(&wdt->feed_work,
			      SMDT_WDT_FEED_INTERVAL * HZ);
}

static const struct watchdog_info smdt_wdt_info = {
	.options = WDIOF_KEEPALIVEPING | WDIOF_MAGICCLOSE,
	.identity = "SMDT MCU Watchdog",
};

static const struct watchdog_ops smdt_wdt_ops = {
	.owner = THIS_MODULE,
	.start = smdt_wdt_start,
	.stop = smdt_wdt_stop,
	.ping = smdt_wdt_ping,
};

static int smdt_wdt_probe(struct i2c_client *client)
{
	struct device *dev = &client->dev;
	struct smdt_wdt *wdt;
	int ret;

	if (!i2c_check_functionality(client->adapter, I2C_FUNC_I2C))
		return dev_err_probe(dev, -EIO, "I2C_FUNC_I2C not supported\n");

	wdt = devm_kzalloc(dev, sizeof(*wdt), GFP_KERNEL);
	if (!wdt)
		return -ENOMEM;

	wdt->client = client;

	INIT_DELAYED_WORK(&wdt->feed_work, smdt_wdt_feed_work);

	wdt->wdd.info = &smdt_wdt_info;
	wdt->wdd.ops = &smdt_wdt_ops;
	wdt->wdd.min_timeout = SMDT_WDT_MIN_TIMEOUT;
	wdt->wdd.max_timeout = SMDT_WDT_MAX_TIMEOUT;
	wdt->wdd.timeout = SMDT_WDT_DEFAULT_TIMEOUT;
	wdt->wdd.parent = dev;

	watchdog_set_drvdata(&wdt->wdd, wdt);
	i2c_set_clientdata(client, wdt);

	watchdog_stop_on_reboot(&wdt->wdd);
	watchdog_stop_on_unregister(&wdt->wdd);
	set_bit(WDOG_HW_RUNNING, &wdt->wdd.status);

	ret = smdt_wdt_start(&wdt->wdd);
	if (ret)
		return dev_err_probe(dev, ret, "failed to start watchdog\n");

	ret = devm_watchdog_register_device(dev, &wdt->wdd);
	if (ret)
		return dev_err_probe(dev, ret, "failed to register watchdog\n");

	dev_info(dev, "SMDT MCU watchdog started (i2c @ 0x%02x)\n",
		 client->addr);

	return 0;
}

static void smdt_wdt_remove(struct i2c_client *client)
{
	struct smdt_wdt *wdt = i2c_get_clientdata(client);

	if (wdt)
		cancel_delayed_work_sync(&wdt->feed_work);
}

static const struct of_device_id smdt_wdt_dt_ids[] = {
	{ .compatible = "smdt,mcu-wdt", },
	{ }
};
MODULE_DEVICE_TABLE(of, smdt_wdt_dt_ids);

static const struct i2c_device_id smdt_wdt_id[] = {
	{ "smdt-mcu-wdt", 0 },
	{ }
};
MODULE_DEVICE_TABLE(i2c, smdt_wdt_id);

static struct i2c_driver smdt_wdt_driver = {
	.driver = {
		.name = "smdt-wdt",
		.of_match_table = smdt_wdt_dt_ids,
	},
	.probe = smdt_wdt_probe,
	.remove = smdt_wdt_remove,
	.id_table = smdt_wdt_id,
};
module_i2c_driver(smdt_wdt_driver);

MODULE_AUTHOR("AIoT-3588IED");
MODULE_DESCRIPTION("SMDT MCU I2C Watchdog Driver");
MODULE_LICENSE("GPL");
