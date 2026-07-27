/**
 * PM2 Ecosystem — 生产环境进程管理。
 *
 * 启动: pm2 start ecosystem.config.js
 * 查看: pm2 status
 * 日志: pm2 logs systrader
 * 停止: pm2 stop ecosystem.config.js
 */

module.exports = {
  apps: [
    {
      name: "systrader",
      script: "shared/runner.py",
      interpreter: "python",
      max_memory_restart: "500M",
      autorestart: true,
      exp_backoff_restart_delay: 5000,
      max_restarts: 10,
      env: {
        PYTHONUNBUFFERED: "1",
      },
      error_file: "logs/pm2-error.log",
      out_file: "logs/pm2-out.log",
      merge_logs: true,
      log_date_format: "YYYY-MM-DD HH:mm:ss",
    },
    {
      name: "dashboard",
      script: "dashboard/server.py",
      interpreter: "python",
      max_memory_restart: "300M",
      autorestart: true,
      env: {
        PYTHONUNBUFFERED: "1",
      },
      error_file: "logs/dashboard-error.log",
      out_file: "logs/dashboard-out.log",
    },
  ],
};
