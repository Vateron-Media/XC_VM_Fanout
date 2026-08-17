# xc_fanout documentation

`xc_fanout` is XC_VM's native live-stream **fan-out** daemon: one process pulls
each source exactly once and fans it out to many viewers through nginx, keeping
PHP out of the byte path, and produces HLS (including AES-128 encrypted) entirely
in memory.

Choose a language / выберите язык:

- 🇬🇧 **[English](en/README.md)**
- 🇷🇺 **[Русский](ru/README.md)**
