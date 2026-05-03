import http from "node:http"
import path from "node:path";

import express from "express"
import { Server } from "socket.io"

async function main() {
    const PORT = process.env.PORT ?? 8000;

    const app = express()
    const server = http.createServer(app)
    const io = new Server()

    io.attach(server)

    app.use(express.static(path.resolve('./public')))

    app.get("/health", (req,res) =>{
        return res.json({healthy: true})
    })
    

    server.listen(PORT, () => {
        console.log(`server running on http://localhost:${PORT}`);
        
    })
}

main()