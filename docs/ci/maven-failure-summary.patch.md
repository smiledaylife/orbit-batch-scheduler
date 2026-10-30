# CI 失败摘要补丁（需仓库维护者手工应用）

> 背景：Arena 沙箱的 GitHub App 没有 `workflows` 权限，无法直接修改 `.github/workflows/maven.yml`，
> 同时也无法下载 Actions 日志（日志托管域名不在沙箱出网白名单内）。
> 应用下面的改动后，构建失败的 `[ERROR]` 行与失败用例会以 **annotation + Step Summary** 形式展示在 Checks 页，
> 并上传完整 `build.log` 产物，便于排错。

将 `Build with Maven` 步骤替换为：

```yaml
      - name: Build with Maven
        shell: bash
        run: |
          set -o pipefail
          mvn -B -ntp verify --file pom.xml 2>&1 | tee build.log

      # 构建失败时，把编译错误 / 失败用例摘要以 annotation 形式贴到 Checks 页
      - name: Summarize build errors
        if: failure()
        shell: bash
        run: |
          grep -E '\[ERROR\]|<<< (FAILURE|ERROR)!|Tests run:.*(Failures: [1-9]|Errors: [1-9])' build.log \
            | grep -vE 'Re-run Maven|For more information|\[Help [0-9]+\]|^\[ERROR\] *$' \
            | head -n 60 > errors.log || true
          if [ -s errors.log ]; then
            split -l 6 -d errors.log chunk_
            for f in chunk_*; do
              msg=$(sed ':a;N;$!ba;s/\n/%0A/g' "$f")
              echo "::error title=Maven build failed::${msg}"
            done
          fi
          { echo '## Maven 构建失败摘要'; echo '```'; cat errors.log; echo '```'; } >> "$GITHUB_STEP_SUMMARY"

      - name: Upload build log
        if: failure()
        uses: actions/upload-artifact@v4
        with:
          name: maven-build-log
          path: build.log
          retention-days: 7
```
