#!/bin/bash
# AutoLive Pro — VPS Agent One-Shot Setup Script
# Run: bash setup.sh
set -e

GREEN='\033[0;32m'
YELLOW='\033[1;33m'
RED='\033[0;31m'
NC='\033[0m'

echo -e "${GREEN}======================================${NC}"
echo -e "${GREEN}  AutoLive Pro VPS Agent — Setup${NC}"
echo -e "${GREEN}======================================${NC}\n"

# ── 1. System packages ──────────────────────────────────────────────────────
echo -e "${YELLOW}[1/6] Installing system packages...${NC}"
sudo apt-get update -qq
sudo apt-get install -y -qq ffmpeg curl

# ── 2. Node.js 20 LTS ───────────────────────────────────────────────────────
if ! command -v node &> /dev/null; then
  echo -e "${YELLOW}[2/6] Installing Node.js 20...${NC}"
  curl -fsSL https://deb.nodesource.com/setup_20.x | sudo -E bash -
  sudo apt-get install -y nodejs
else
  echo -e "${GREEN}[2/6] Node.js already installed: $(node -v)${NC}"
fi

# ── 3. PM2 ──────────────────────────────────────────────────────────────────
if ! command -v pm2 &> /dev/null; then
  echo -e "${YELLOW}[3/6] Installing PM2...${NC}"
  sudo npm install -g pm2 --quiet
else
  echo -e "${GREEN}[3/6] PM2 already installed: $(pm2 -v | tail -1)${NC}"
fi

# ── 4. npm install ──────────────────────────────────────────────────────────
echo -e "${YELLOW}[4/6] Installing Node dependencies...${NC}"
npm install --quiet

# ── 5. .env setup ───────────────────────────────────────────────────────────
echo -e "${YELLOW}[5/6] Setting up .env...${NC}"
if [ ! -f .env ]; then
  cp .env.example .env
  echo ""
  echo -e "${RED}⚠  .env created from example. Edit it now:${NC}"
  echo -e "${YELLOW}   nano .env${NC}"
  echo ""
  echo "   Required fields:"
  echo "   PORT=5001"
  echo "   API_KEY=<your-secret-key>   ← same as VPS_API_KEY in backend"
  echo "   MAX_STREAMS=5"
  echo ""
  read -p "Press Enter after you've saved .env to continue..." _
else
  echo -e "${GREEN}   .env already exists — skipping${NC}"
fi

# Validate API_KEY is set
source .env
if [ -z "$API_KEY" ] || [ "$API_KEY" = "CHANGE_ME" ]; then
  echo -e "${RED}ERROR: API_KEY is not set in .env. Edit .env and re-run setup.sh${NC}"
  exit 1
fi

# ── 6. Firewall ─────────────────────────────────────────────────────────────
echo -e "${YELLOW}[6/6] Configuring firewall (ufw)...${NC}"
sudo ufw allow OpenSSH  2>/dev/null || true
sudo ufw allow "${PORT:-5001}/tcp" 2>/dev/null || true
sudo ufw --force enable 2>/dev/null || true

# ── Start with PM2 ──────────────────────────────────────────────────────────
echo ""
echo -e "${YELLOW}Starting agent with PM2...${NC}"
pm2 stop autolive-agent 2>/dev/null || true
pm2 start ecosystem.config.js
pm2 save

# Auto-start on reboot
STARTUP_CMD=$(pm2 startup 2>&1 | grep "sudo env" || true)
if [ -n "$STARTUP_CMD" ]; then
  echo -e "${YELLOW}Running PM2 startup command...${NC}"
  eval "$STARTUP_CMD"
fi

# ── Done ────────────────────────────────────────────────────────────────────
echo ""
echo -e "${GREEN}======================================${NC}"
echo -e "${GREEN}  Setup Complete!${NC}"
echo -e "${GREEN}======================================${NC}"
echo ""
echo "  Health check:"
echo "  curl http://localhost:${PORT:-5001}/health"
echo ""
echo "  Status:"
echo "  curl -H \"x-api-key: ${API_KEY}\" http://localhost:${PORT:-5001}/stream/status"
echo ""
echo "  PM2 logs:"
echo "  pm2 logs autolive-agent"
echo ""
echo -e "${YELLOW}⚠  Don't forget: Open port ${PORT:-5001} in Oracle Security List!${NC}"
echo ""
