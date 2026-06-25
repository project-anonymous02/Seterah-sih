import { useState, useEffect, useRef } from "react";
import { createClient } from "https://cdn.jsdelivr.net/npm/@supabase/supabase-js@2/+esm";

const supabase = createClient(
  "https://wnbgbysecuijrfuawjlp.supabase.co",
  "eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJpc3MiOiJzdXBhYmFzZSIsInJlZiI6InduYmdieXNlY3VpanJmdWF3amxwIiwicm9sZSI6ImFub24iLCJpYXQiOjE3ODIzODM5MjYsImV4cCI6MjA5Nzk1OTkyNn0.3-wkM65a16mSToiehv3cVUSx6eVJJTqMhLn3aXZVdAw"
);

const adjectives = ["silent","cosmic","hollow","vivid","broken","neon","ghost","pale","burning","lost"];
const nouns = ["echo","void","moth","tide","ember","drift","shard","pulse","rift","haze"];
const randomName = () => `${adjectives[Math.floor(Math.random()*adjectives.length)]}_${nouns[Math.floor(Math.random()*nouns.length)]}`;

function timeAgo(date) {
  const s = Math.floor((Date.now() - new Date(date)) / 1000);
  if (s < 60) return `${s}s ago`;
  if (s < 3600) return `${Math.floor(s/60)}m ago`;
  if (s < 86400) return `${Math.floor(s/3600)}h ago`;
  return `${Math.floor(s/86400)}d ago`;
}

export default function App() {
  const [comments, setComments] = useState([]);
  const [content, setContent] = useState("");
  const [username] = useState(() => {
    return sessionStorage.getItem("comments_username") || (() => {
      const n = randomName();
      sessionStorage.setItem("comments_username", n);
      return n;
    })();
  });
  const [loading, setLoading] = useState(true);
  const [posting, setPosting] = useState(false);
  const [liveCount, setLiveCount] = useState(0);
  const [upvoted, setUpvoted] = useState(() => {
    try { return JSON.parse(sessionStorage.getItem("upvoted") || "{}"); } catch { return {}; }
  });
  const bottomRef = useRef(null);
  const textareaRef = useRef(null);

  useEffect(() => {
    // fetch initial
    supabase
      .from("comments")
      .select("*")
      .order("created_at", { ascending: true })
      .then(({ data }) => {
        setComments(data || []);
        setLoading(false);
      });

    // realtime subscription
    const channel = supabase
      .channel("comments-room")
      .on("postgres_changes", { event: "INSERT", schema: "public", table: "comments" }, (payload) => {
        setComments(prev => [...prev, payload.new]);
      })
      .on("postgres_changes", { event: "UPDATE", schema: "public", table: "comments" }, (payload) => {
        setComments(prev => prev.map(c => c.id === payload.new.id ? payload.new : c));
      })
      .on("presence", { event: "sync" }, () => {
        setLiveCount(Object.keys(channel.presenceState()).length);
      })
      .subscribe(async (status) => {
        if (status === "SUBSCRIBED") {
          await channel.track({ user: username, online_at: new Date().toISOString() });
        }
      });

    return () => { supabase.removeChannel(channel); };
  }, []);

  useEffect(() => {
    if (!loading) bottomRef.current?.scrollIntoView({ behavior: "smooth" });
  }, [comments.length]);

  const handlePost = async () => {
    if (!content.trim() || posting) return;
    setPosting(true);
    await supabase.from("comments").insert({ username, content: content.trim() });
    setContent("");
    setPosting(false);
    textareaRef.current?.focus();
  };

  const handleUpvote = async (comment) => {
    if (upvoted[comment.id]) return;
    const newUpvoted = { ...upvoted, [comment.id]: true };
    setUpvoted(newUpvoted);
    sessionStorage.setItem("upvoted", JSON.stringify(newUpvoted));
    await supabase.from("comments").update({ upvotes: comment.upvotes + 1 }).eq("id", comment.id);
  };

  const handleKey = (e) => {
    if (e.key === "Enter" && (e.ctrlKey || e.metaKey)) handlePost();
  };

  return (
    <div style={{
      minHeight: "100vh",
      background: "#0a0a0f",
      color: "#e2e2e8",
      fontFamily: "'Inter', system-ui, sans-serif",
      display: "flex",
      flexDirection: "column",
    }}>
      {/* Header */}
      <header style={{
        borderBottom: "1px solid #1e1e2e",
        padding: "16px 24px",
        display: "flex",
        alignItems: "center",
        justifyContent: "space-between",
        position: "sticky",
        top: 0,
        background: "rgba(10,10,15,0.95)",
        backdropFilter: "blur(12px)",
        zIndex: 10,
      }}>
        <div style={{ display: "flex", alignItems: "center", gap: "12px" }}>
          <div style={{
            width: 32, height: 32,
            background: "linear-gradient(135deg, #7c3aed, #2563eb)",
            borderRadius: 8,
            display: "flex", alignItems: "center", justifyContent: "center",
            fontSize: 16,
          }}>💬</div>
          <span style={{ fontWeight: 700, fontSize: 18, letterSpacing: "-0.5px" }}>Comments</span>
        </div>
        <div style={{ display: "flex", alignItems: "center", gap: "16px" }}>
          <div style={{ display: "flex", alignItems: "center", gap: "6px", fontSize: 13, color: "#6b6b80" }}>
            <span style={{
              width: 7, height: 7, borderRadius: "50%",
              background: liveCount > 0 ? "#22c55e" : "#4b4b60",
              display: "inline-block",
              boxShadow: liveCount > 0 ? "0 0 6px #22c55e" : "none",
            }} />
            {liveCount} online
          </div>
          <div style={{
            fontSize: 13,
            color: "#7c7c9a",
            background: "#13131f",
            border: "1px solid #1e1e2e",
            borderRadius: 20,
            padding: "4px 12px",
          }}>
            👤 {username}
          </div>
        </div>
      </header>

      {/* Comments */}
      <main style={{ flex: 1, overflowY: "auto", padding: "24px 0", maxWidth: 720, width: "100%", margin: "0 auto", boxSizing: "border-box" }}>
        {loading ? (
          <div style={{ textAlign: "center", padding: 60, color: "#4b4b60" }}>
            <div style={{ fontSize: 32, marginBottom: 12 }}>◌</div>
            loading comments...
          </div>
        ) : comments.length === 0 ? (
          <div style={{ textAlign: "center", padding: 60, color: "#4b4b60" }}>
            <div style={{ fontSize: 40, marginBottom: 12 }}>🌑</div>
            <div style={{ fontSize: 15 }}>nothing here yet.</div>
            <div style={{ fontSize: 13, marginTop: 6 }}>be the first to say something.</div>
          </div>
        ) : (
          <div style={{ display: "flex", flexDirection: "column", gap: "1px" }}>
            {comments.map((c, i) => (
              <div key={c.id} style={{
                padding: "18px 24px",
                borderBottom: "1px solid #13131f",
                display: "flex",
                gap: 16,
                animation: i === comments.length - 1 ? "fadeIn 0.3s ease" : "none",
              }}>
                {/* Upvote */}
                <div style={{ display: "flex", flexDirection: "column", alignItems: "center", gap: 4, minWidth: 36 }}>
                  <button
                    onClick={() => handleUpvote(c)}
                    style={{
                      background: "none",
                      border: "none",
                      cursor: upvoted[c.id] ? "default" : "pointer",
                      fontSize: 18,
                      padding: 4,
                      borderRadius: 6,
                      color: upvoted[c.id] ? "#7c3aed" : "#3a3a50",
                      transition: "color 0.15s, transform 0.1s",
                      transform: upvoted[c.id] ? "scale(1.1)" : "scale(1)",
                    }}
                  >▲</button>
                  <span style={{ fontSize: 13, fontWeight: 600, color: upvoted[c.id] ? "#7c3aed" : "#4b4b60" }}>
                    {c.upvotes}
                  </span>
                </div>

                {/* Content */}
                <div style={{ flex: 1 }}>
                  <div style={{ display: "flex", alignItems: "center", gap: 10, marginBottom: 8 }}>
                    <span style={{
                      fontSize: 13,
                      fontWeight: 600,
                      color: c.username === username ? "#7c3aed" : "#5a5a78",
                    }}>
                      {c.username === username ? `${c.username} (you)` : c.username}
                    </span>
                    <span style={{ fontSize: 12, color: "#3a3a50" }}>{timeAgo(c.created_at)}</span>
                  </div>
                  <p style={{
                    margin: 0,
                    fontSize: 15,
                    lineHeight: 1.6,
                    color: "#c8c8d8",
                    wordBreak: "break-word",
                  }}>{c.content}</p>
                </div>
              </div>
            ))}
          </div>
        )}
        <div ref={bottomRef} />
      </main>

      {/* Input */}
      <div style={{
        borderTop: "1px solid #1e1e2e",
        padding: "16px 24px",
        background: "rgba(10,10,15,0.98)",
        backdropFilter: "blur(12px)",
        maxWidth: 720,
        width: "100%",
        margin: "0 auto",
        boxSizing: "border-box",
      }}>
        <div style={{
          display: "flex",
          gap: 12,
          alignItems: "flex-end",
          background: "#13131f",
          border: "1px solid #1e1e2e",
          borderRadius: 12,
          padding: "12px 16px",
          transition: "border-color 0.2s",
        }}>
          <textarea
            ref={textareaRef}
            value={content}
            onChange={e => setContent(e.target.value)}
            onKeyDown={handleKey}
            placeholder="say something..."
            rows={1}
            style={{
              flex: 1,
              background: "none",
              border: "none",
              outline: "none",
              color: "#e2e2e8",
              fontSize: 15,
              fontFamily: "inherit",
              resize: "none",
              lineHeight: 1.5,
              maxHeight: 120,
              overflow: "auto",
            }}
            onInput={e => {
              e.target.style.height = "auto";
              e.target.style.height = Math.min(e.target.scrollHeight, 120) + "px";
            }}
          />
          <button
            onClick={handlePost}
            disabled={!content.trim() || posting}
            style={{
              background: content.trim() ? "linear-gradient(135deg, #7c3aed, #2563eb)" : "#1e1e2e",
              border: "none",
              color: content.trim() ? "#fff" : "#3a3a50",
              borderRadius: 8,
              padding: "8px 16px",
              fontSize: 14,
              fontWeight: 600,
              cursor: content.trim() ? "pointer" : "default",
              transition: "all 0.15s",
              whiteSpace: "nowrap",
            }}
          >
            {posting ? "..." : "Post"}
          </button>
        </div>
        <div style={{ fontSize: 12, color: "#2e2e42", marginTop: 8, textAlign: "right" }}>
          Ctrl+Enter to post
        </div>
      </div>

      <style>{`
        @keyframes fadeIn {
          from { opacity: 0; transform: translateY(8px); }
          to { opacity: 1; transform: translateY(0); }
        }
        * { box-sizing: border-box; }
        ::-webkit-scrollbar { width: 4px; }
        ::-webkit-scrollbar-track { background: transparent; }
        ::-webkit-scrollbar-thumb { background: #1e1e2e; border-radius: 4px; }
      `}</style>
    </div>
  );
}
