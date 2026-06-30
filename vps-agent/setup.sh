#!/bin/bash
# AutoLive Pro — VPS Agent v2 Setup Script
set -e

GREEN='\033[0;32m'; YELLOW='\033[1;33m'; RED='\033[0;31m'; NC='\033[0m'

echo -e "${GREEN}======================================${NC}"
echo -e "${GREEN}  AutoLive Pro VPS Agent v2 — Setup  ${NC}"
echo -e "${GREEN}======================================${NC}\n"

# 1. System packages
echo -e "${YELLOW}[1/6] Installing system packages...${NC}"
sudo apt-get update -qq
sudo apt-get install -y -qq ffmpeg curl wget

# 2. Node.js 20 LTS
if ! command -v node &> /dev/null; then
  echo -e "${YELLOW}[2/6] Installing Node.js 20...${NC}"
  curl -fsSL https://deb.nodesource.com/setup_20.x | sudo -E bash -
  sudo apt-get install -y nodejs
else
  echo -e "${GREEN}[2/6] Node.js already installed: $(node -v)${NC}"
fi

# 3. PM2
if ! command -v pm2 &> /dev/null; then
  echo -e "${YELLOW}[3/6] Installing PM2...${NC}"
  sudo npm install -g pm2 --quiet
else
  echo -e "${GREEN}[3/6] PM2 already installed${NC}"
fi

# 4. npm install
echo -e "${YELLOW}[4/6] Installing Node dependencies...${NC}"
npm install --quiet

# 5. .env setup
echo -e "${YELLOW}[5/6] Setting up .env...${NC}"
if [ ! -f .env ]; then
  cp .env.example .env
  echo ""
  echo -e "${RED}⚠  Edit .env now:${NC}"
  echo "   nano .env"
  echo ""
  echo "   PORT=5001"
  echo "   API_KEY=<same as what you enter in Admin Panel>"
  echo "   CONSOLE_PIN=<pin for terminal access>"
  echo "   MAX_STREAMS=15"
  echo ""
  read -p "Press Enter after saving .env..." _
fi

source .env
if [ -z "$API_KEY" ] || [ "$API_KEY" = "your_secret_api_key_here" ]; then
  echo -e "${RED}ERROR: API_KEY not set in .env${NC}"
  exit 1
fi

# 6. Firewall
echo -e "${YELLOW}[6/6] Firewall setup...${NC}"
sudo ufw allow OpenSSH  2>/dev/null || true
sudo ufw allow "${PORT:-5001}/tcp" 2>/dev/null || true
sudo ufw --force enable 2>/dev/null || true

# Start with PM2
echo ""
echo -e "${YELLOW}Starting agent with PM2...${NC}"
pm2 stop autolive-agent 2>/dev/null || true
pm2 start ecosystem.config.js
pm2 save

STARTUP=$(pm2 startup 2>&1 | grep "sudo env" || true)
[ -n "$STARTUP" ] && eval "$STARTUP"

echo ""
echo -e "${GREEN}======================================${NC}"
echo -e "${GREEN}  ✅ Setup Complete!                  ${NC}"
echo -e "${GREEN}======================================${NC}"
echo ""
echo "  Health check:"
echo "  curl http://localhost:${PORT:-5001}/health"
echo ""
echo -e "${YELLOW}⚠  Open port ${PORT:-5001} in Oracle Security List!${NC}"
echo -e "${YELLOW}⚠  In Admin Panel → VPS Servers → Add:${NC}"
echo "   URL: http://YOUR_VPS_IP:${PORT:-5001}"
echo "   API Key: $API_KEY"
echo "   Max Streams: ${MAX_STREAMS:-15}"
echo ""
