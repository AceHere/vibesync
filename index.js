const express = require('express');
const http = require('http');
const { Server } = require('socket.io');
const cors = require('cors');

const app = express();
app.use(cors());

const server = http.createServer(app);

const io = new Server(server, {
  cors: {
    origin: '*',
    methods: ['GET', 'POST']
  }
});

io.on('connection', (socket) => {
  console.log(`User connected: ${socket.id}`);

  // User joins a specific room
  socket.on('join-room', (roomId) => {
    socket.join(roomId);
    console.log(`User ${socket.id} joined room ${roomId}`);
    
    // Notify others in the room that a new user joined
    socket.to(roomId).emit('user-joined', socket.id);
  });

  // WebRTC Signaling
  socket.on('webrtc-offer', (data) => {
    socket.to(data.target).emit('webrtc-offer', {
      sender: socket.id,
      sdp: data.sdp
    });
  });

  socket.on('webrtc-answer', (data) => {
    socket.to(data.target).emit('webrtc-answer', {
      sender: socket.id,
      sdp: data.sdp
    });
  });

  socket.on('webrtc-ice-candidate', (data) => {
    socket.to(data.target).emit('webrtc-ice-candidate', {
      sender: socket.id,
      candidate: data.candidate
    });
  });

  // Video Syncing
  socket.on('video-state', (data) => {
    // data should contain { roomId, type: 'play' | 'pause' | 'seek', time: number }
    socket.to(data.roomId).emit('video-state', {
      sender: socket.id,
      ...data
    });
  });

  // Text Chat
  socket.on('chat-message', (data) => {
    // data should contain { roomId, message, senderName }
    socket.to(data.roomId).emit('chat-message', {
      sender: socket.id,
      ...data
    });
  });

  socket.on('disconnect', () => {
    console.log(`User disconnected: ${socket.id}`);
    // Optional: emit user-left event to all rooms the user was in
  });
});

const PORT = process.env.PORT || 3000;
server.listen(PORT, () => {
  console.log(`Watch Party Signaling Server running on port ${PORT}`);
});
