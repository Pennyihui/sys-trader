// 测试 Script.js 的 main 函数生成逻辑
const fs = require('fs');

// 读取 Script.js 源码
const src = fs.readFileSync(
  'C:/Users/Evan/AppData/Roaming/io.github.clash-verge-rev.clash-verge-rev/profiles/Script.js',
  'utf8'
);

// 提取 main 函数并执行
const main = new Function('config', 'profileName', src + '\nreturn main;')();

// 构造测试 config
const proxies = JSON.parse(fs.readFileSync('_test_proxies.json', 'utf8'));
const config = {
  proxies: proxies,
  'proxy-groups': [],
  rules: ['MATCH,auto'],
};

const result = main(config, 'test');

console.log('proxy-groups 数量:', result['proxy-groups'].length);
console.log('listeners 数量:', result.listeners.length);
for (const g of result['proxy-groups']) {
  console.log(`  组 ${g.name}: ${g.proxies.length} 节点`);
}
for (const l of result.listeners) {
  console.log(`  监听 ${l.name}: 端口${l.port} -> ${l.proxy}`);
}
console.log('rules 前3条:', result.rules.slice(0, 3));
