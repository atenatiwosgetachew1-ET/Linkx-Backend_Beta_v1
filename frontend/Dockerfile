FROM node:18

WORKDIR /app

# Copy only dependency files
COPY package.json package-lock.json* ./

RUN npm install

# Do NOT copy source code for dev
CMD ["npm", "run", "dev", "--", "--host"]
