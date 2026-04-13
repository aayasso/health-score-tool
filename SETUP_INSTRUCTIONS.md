# Claude Code Setup — Mac Instructions
## LaSalle Technologies / Health Score Suite

---

## Before You Start — What You Need

- Your Mac
- Your `health-score-tool` GitHub repo already cloned on your Mac
- The 7 downloaded `.md` files from this session (`CLAUDE.md`, `AGENTS.md`, `TESTING.md`, `TOOL_SPECS.md`, `CONTEXT.md`, `ARCHITECTURE.md`, `SESSION_KICKOFF.md`)
- Claude Pro subscription (you already have this)

---

## One-Time Setup (do this once, never again)

### Step 1 — Install Claude Code

1. Go to `https://claude.ai/download` in your browser
2. Download the Mac version
3. Open the downloaded file and drag Claude Code to your Applications folder
4. Open Claude Code from Applications and sign in with your Anthropic account

### Step 2 — Install the Terminal Command

1. With Claude Code open, click the menu bar: **Claude Code → Install CLI Tool**
2. Click **Install** and enter your Mac password if prompted
3. Open Terminal (`⌘ + Space` → type `Terminal` → Enter)
4. Type `claude --version` and press Enter
5. You should see a version number printed. If you see "command not found", quit Terminal, reopen it, and try again.

### Step 3 — Put the MD Files in Your Repo

1. Open Terminal
2. Type this and press Enter (replace `your-username` with your GitHub username):
   ```
   cd ~/health-score-tool
   ```
   If your repo is in a different location, use that path instead.
3. Move the downloaded files into the repo. Run these one at a time:
   ```
   mv ~/Downloads/CLAUDE.md .
   mv ~/Downloads/AGENTS.md .
   mv ~/Downloads/TESTING.md .
   mv ~/Downloads/TOOL_SPECS.md .
   mv ~/Downloads/CONTEXT.md .
   mv ~/Downloads/ARCHITECTURE.md .
   mv ~/Downloads/SESSION_KICKOFF.md .
   ```
4. Confirm they landed correctly:
   ```
   ls *.md
   ```
   You should see all 7 files listed.

### Step 4 — Protect the Proprietary File

`TOOL_SPECS.md` contains your weights and must never be pushed to GitHub.

1. Open the file called `.gitignore` in your repo (use TextEdit or any text editor)
2. If `.gitignore` does not exist, create it in the repo root folder
3. Add these lines at the bottom and save:
   ```
   TOOL_SPECS.md
   utils/normalization.py
   utils/scoring.py
   data/rasters/*.tif
   data/rasters/*.tiff
   ```

### Step 5 — Commit the Agent Files to GitHub

In Terminal (you should still be in your repo folder):

```
git add CLAUDE.md AGENTS.md TESTING.md CONTEXT.md ARCHITECTURE.md SESSION_KICKOFF.md
git commit -m "Add Claude Code agent context files"
git push origin main
```

Note: `TOOL_SPECS.md` is intentionally left out of this commit. Keep it only on your local machine.

**Setup is complete. You will never need to do Steps 1–5 again.**

---

## Every Session — Start to Finish

### Starting a Session

1. Open Terminal
2. Type this and press Enter:
   ```
   cd ~/health-score-tool
   ```
3. Type this and press Enter:
   ```
   claude
   ```
4. Open `SESSION_KICKOFF.md` in TextEdit
5. Copy the entire kickoff prompt
6. Paste it into the Claude Code terminal window and press Enter
7. Claude Code will read all the context files and show you a plan
8. Read the plan. If it looks right, type `approved` and press Enter. If something is off, tell it what to change before approving.

### During a Session

- Claude Code will stop and show you a `⚠️ DECISION NEEDED` message whenever it hits something that requires your input
- Read it, pick an option or give your own direction, and press Enter
- It will not proceed until you respond — you are always in control

### Ending a Session

1. Paste this into Claude Code and press Enter:
   ```
   End of session. Summarize what was completed, where we left off, what the next session should start with, and any unresolved issues. Then update CONTEXT.md with a new session log entry.
   ```
2. Wait for Claude Code to finish updating `CONTEXT.md`
3. In Terminal, run:
   ```
   git add CONTEXT.md
   git commit -m "Session log update"
   git push origin main
   ```
4. Type `exit` in Claude Code and press Enter, or press `Ctrl + C`

---

## If Something Goes Wrong

| Problem | Fix |
|---|---|
| `claude: command not found` | Quit Terminal, reopen it, try `claude --version` again. If still broken, redo Step 2. |
| Claude Code starts but ignores the plan format | Paste: `Stop. Read CLAUDE.md Step 3 and produce a written plan before doing anything else.` |
| Claude Code is guessing instead of asking you | Paste: `Stop. Read CLAUDE.md Step 4. You must prompt me for any decision before proceeding.` |
| Git push fails | Run `git pull origin main` first, then push again. |
| Claude Code terminal freezes | Press `Ctrl + C` to cancel, then type `claude` to restart. Your repo files are safe. |
