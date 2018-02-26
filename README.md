# luigi_practice

luigiの練習


```
cd tasks
sudo luigid --background
python3 run_luigi.py Beta --sec 66
```

## 外部コマンドの実行

`from luigi.contrib.external_program import ExternalProgramTask` を使うと外部コマンドを実行できるが、パイプやリダイレクトができない。
それらの操作をしたいときのために、 luigiの作者はplumbum モジュールの使用を提案している。(実行できればなんでも良さそうなのでsuprocess使ってもよさそう。)

```
cd tasks
python3 external_luigi.py Beta
```

## 可視化

```
luigid
```

luigidコマンドで可視化のためのサーバを立てた状態でluigiスクリプトを実行する。

デフォルトで http://localhost:8082 で見れる。

![](./example.png)
![](./example2.png)
