import { useState, useEffect, useRef } from 'react';
import { useNavigate, useLocation } from 'react-router-dom';
import { 
  Home, 
  Search, 
  Library, 
  Disc3, 
  Play, 
  Pause, 
  SkipBack, 
  SkipForward, 
  Mic2, 
  Volume2, 
  Info,
  ChevronLeft,
  ChevronRight,
  Music,
  Heart,
  User as UserIcon,
  ChevronDown,
  Globe,
  Calendar,
  Bell
} from 'lucide-react';
import './index.css';

interface Song {
  id: number;
  name: string;
  artist: string;
  score?: number;
  position?: number;
  number?: string;
}

interface UserProfile {
  id: number;
  username: string;
  age: number;
  country: string;
}

interface Notification {
  id: number;
  message: string;
}

interface UserNotification {
  artist_id: number;
  artist_name: string;
  release_id: number;
  release_name: string;
  notified_at: string;
}

interface AlbumTracks {
  release_id: number;
  release_name: string;
  tracks: Song[];
}

function App() {
  const navigate = useNavigate();
  const location = useLocation();
  const activeTab = location.pathname.substring(1) || 'home';

  const [activeUser, setActiveUser] = useState<UserProfile | null>(null);
  const [users, setUsers] = useState<UserProfile[]>([]);
  const [songs, setSongs] = useState<Song[]>([]);
  const [searchResults, setSearchResults] = useState<Song[]>([]);
  const [playlist, setPlaylist] = useState<Song[]>([]);
  const [friendHistory, setFriendHistory] = useState<Song[]>([]);
  const [userNotifications, setUserNotifications] = useState<UserNotification[]>([]);
  const [userFriends, setUserFriends] = useState<UserProfile[]>([]);
  const [artistSongs, setArtistSongs] = useState<Song[]>([]);
  const [selectedArtist, setSelectedArtist] = useState<string | null>(null);
  const [source, setSource] = useState<string>("");
  const [loading, setLoading] = useState<boolean>(false);
  const [message, setMessage] = useState<string>("");
  const [currentSong, setCurrentSong] = useState<Song | null>(null);
  const [isPlaying, setIsPlaying] = useState<boolean>(false);
  const [likedSongKeys, setLikedSongKeys] = useState<Set<string>>(new Set());
  const [searchQuery, setSearchQuery] = useState<string>("");
  const [isUserMenuOpen, setIsUserMenuOpen] = useState<boolean>(false);
  const [currentTime, setCurrentTime] = useState<number>(0);
  const [duration] = useState<number>(30); // 30 seconds default for demo
  const [notifications, setNotifications] = useState<Notification[]>([]);
  const [timeOfDay, setTimeOfDay] = useState<string>("morning");
  const [isFetching, setIsFetching] = useState<boolean>(false);

  const [queue, setQueue] = useState<Song[]>([]);
  const [queueIndex, setQueueIndex] = useState<number>(0);
  const [selectedAlbum, setSelectedAlbum] = useState<AlbumTracks | null>(null);
  const [albumLoading, setAlbumLoading] = useState<boolean>(false);
  const [selectedProfile, setSelectedProfile] = useState<UserProfile | null>(null);
  const [profileTopTracks, setProfileTopTracks] = useState<Song[]>([]);
  const [profileLoading, setProfileLoading] = useState<boolean>(false);

  const [isNotifMenuOpen, setIsNotifMenuOpen] = useState<boolean>(false);
  const [notifSeen, setNotifSeen] = useState<boolean>(false);

  const usersFetchedRef = useRef(false);
  const activeUserRef = useRef<UserProfile | null>(null);
  useEffect(() => {
    activeUserRef.current = activeUser;
  }, [activeUser]);

  const handleTabChange = (newTab: string) => {
    navigate(`/${newTab === 'home' ? '' : newTab}`);
  };

  const handleBack = () => navigate(-1);
  const handleForward = () => navigate(1);

  const formatTime = (seconds: number) => {
    const mins = Math.floor(seconds / 60);
    const secs = Math.floor(seconds % 60);
    return `${mins}:${secs.toString().padStart(2, "0")}`;
  };

  const songKey = (song: Song) => `${song.id}:${song.name}:${song.artist}`;
  const isCurrentSong = (song: Song) => currentSong ? songKey(currentSong) === songKey(song) : false;

  const fetchWithTimeout = async (url: string, options: RequestInit = {}, timeoutMs = 60000) => {
    const controller = new AbortController();
    const timeoutId = window.setTimeout(() => controller.abort(), timeoutMs);

    try {
      return await fetch(url, { cache: "no-store", ...options, signal: controller.signal });
    } finally {
      window.clearTimeout(timeoutId);
    }
  };

  const notifCounterRef = useRef(0);
  // WebSocket for notifications
 useEffect(() => {
  const ws = new WebSocket("ws://localhost:8000/ws");
  ws.onmessage = (event) => {
    try {
      const payload = JSON.parse(event.data);
      if (payload.type === "kafka_event") {
        const { user_id, recording_id } = payload.data;
        const notifId = ++notifCounterRef.current;
        const newNotif = {
          id: notifId,
          message: `Kafka: User ${user_id} played Song ${recording_id}`
        };
        setNotifications(prev => [newNotif, ...prev].slice(0, 5));
        setTimeout(() => {
          setNotifications(prev => prev.filter(n => n.id !== notifId));
        }, 5000);
      } else if (payload.type === "broadcast_play") {
        const currentUserId = String(activeUserRef.current?.id);
        const song = payload.assignments?.[currentUserId];
        if (song) {
          setCurrentSong(song);
          setIsPlaying(true);
          setCurrentTime(0);
          const notifId = ++notifCounterRef.current;
          const newNotif = {
            id: notifId,
            message: `🎵 Broadcast: "${song.name}" — ${song.artist}`
          };
          setNotifications(prev => [newNotif, ...prev].slice(0, 5));
          setTimeout(() => {
            setNotifications(prev => prev.filter(n => n.id !== notifId));
          }, 6000);
        }
      } else if (payload.type === "new_notification") {
        if (String(payload.user_id) === String(activeUserRef.current?.id)) {
          setNotifSeen(false);
          const notifId = ++notifCounterRef.current;
          const newNotif = {
            id: notifId,
            message: `🔔 ${payload.artist_name} lançou "${payload.release_name}"`
          };
          setNotifications(prev => [newNotif, ...prev].slice(0, 5));
          setTimeout(() => {
            setNotifications(prev => prev.filter(n => n.id !== notifId));
          }, 6000);
          fetchNotifications();
        }
      }
    } catch (err) {
      console.error("WebSocket message error:", err);
    }
  };
  return () => ws.close();
}, []); // continua sem dependências — o ref resolve o stale closure

  // Slider effect
  useEffect(() => {
    let interval: any;
    if (isPlaying && currentSong) {
      interval = setInterval(() => {
        setCurrentTime((prev) => {
          if (prev + 1 >= duration) {
            window.setTimeout(() => {
              playNext();
            }, 0);
            return duration;
          }
          return prev + 1;
        });
      }, 1000);
    } else {
      clearInterval(interval);
    }
    return () => clearInterval(interval);
  }, [isPlaying, currentSong, duration, queueIndex, queue]);

  // Fetch initial users
  useEffect(() => {
    if (usersFetchedRef.current) return;
    usersFetchedRef.current = true;

    const fetchUsers = async () => {
      setLoading(true);
      try {
        const res = await fetch("http://localhost:8000/users", { cache: "no-store" });
        const data = await res.json();
        const userList = Array.isArray(data) ? data : [];
        setUsers(userList);
        if (userList.length > 0) setActiveUser(userList[0]);
      } catch (err) {
        console.error("Failed to fetch users", err);
        setUsers([]);
      } finally {
        setLoading(false);
      }
    };
    fetchUsers();
  }, []);

  const fetchUserFriends = async () => {
    if (!activeUser) return;
    try {
      const res = await fetchWithTimeout(`http://localhost:8000/friends/${activeUser.id}/list`);
      const data = await res.json();
      setUserFriends(Array.isArray(data) ? data : []);
    } catch (err) {
      console.error("User friends failed", err);
      setUserFriends([]);
    }
  };

  const fetchAllUserData = async () => {
    if (!activeUser) return;
    setIsFetching(true);
    
    // Clear previous data to prevent ghosting
    setSongs([]);
    setPlaylist([]);
    setFriendHistory([]);
    setUserNotifications([]);
    setUserFriends([]);
    setArtistSongs([]);
    setSelectedArtist(null);
    setSelectedAlbum(null);
    setSelectedProfile(null);
    setProfileTopTracks([]);
    setCurrentSong(null);
    setIsPlaying(false);

    try {
      await Promise.allSettled([
        fetchHome(),
        fetchPlaylist(timeOfDay),
        fetchFriends(),
        fetchNotifications(),
        fetchUserFriends()
      ]);
    } finally {
      setIsFetching(false);
    }
  };

  // Fetch everything when user changes
  useEffect(() => {
    fetchAllUserData();
  }, [activeUser]);

  // Fetch only playlist when time of day changes
  useEffect(() => {
    if (activeUser) {
      const loadPlaylist = async () => {
        setIsFetching(true);
        setPlaylist([]); // Clear old playlist before fetching new one
        try {
          await fetchPlaylist(timeOfDay);
        } finally {
          setIsFetching(false);
        }
      };
      loadPlaylist();
    }
  }, [timeOfDay]);

  // Search logic
  useEffect(() => {
    console.log("Search query changed:", searchQuery);
    const delayDebounceFn = setTimeout(async () => {
      if (searchQuery && searchQuery.trim().length >= 2) {
        console.log("Calling fetchSearch with:", searchQuery);
        setIsFetching(true);
        try {
          await fetchSearch(searchQuery.trim());
        } finally {
          setIsFetching(false);
        }
      } else {
        setSearchResults([]);
      }
    }, 500);

    return () => clearTimeout(delayDebounceFn);
  }, [searchQuery]);

  const fetchSearch = async (query: string) => {
    try {
      const res = await fetchWithTimeout(`http://localhost:8000/search?q=${encodeURIComponent(query)}`);
      if (!res.ok) {
        console.error("Search API returned error:", res.status);
        return;
      }
      const data = await res.json();
      console.log("Search results received:", data.results);
      setSearchResults(data.results || []);
    } catch (err) {
      console.error("Search failed", err);
      setSearchResults([]);
    }
  };

  const fetchPlaylist = async (tod: string) => {
    if (!activeUser) return;
    try {
      const res = await fetchWithTimeout(`http://localhost:8000/playlist/${activeUser.id}?time_of_day=${tod}`);
      const data = await res.json();
      setPlaylist(data.songs || []);
    } catch (err) {
      console.error("Playlist fetch failed", err);
      setPlaylist([]);
    }
  };

  const fetchFriends = async () => {
    if (!activeUser) return;
    try {
      const res = await fetchWithTimeout(`http://localhost:8000/friends/${activeUser.id}/history`);
      const data = await res.json();
      setFriendHistory(Array.isArray(data) ? data : []);
    } catch (err) {
      console.error("Friend history failed", err);
      setFriendHistory([]);
    }
  };

  const fetchNotifications = async () => {
    if (!activeUser) return;
    try {
      const res = await fetchWithTimeout(`http://localhost:8000/notifications/${activeUser.id}`);
      if (!res.ok) {
        console.error("Notifications API returned error:", res.status);
        return;
      }
      const data = await res.json();
      setUserNotifications(Array.isArray(data) ? data : []);
    } catch (err) {
      console.error("Notifications fetch failed", err);
      setUserNotifications([]);
      setNotifSeen(false);
    }
  };

  const fetchArtistSongs = async (artistId: number, artistName: string) => {
    setSelectedArtist(artistName);
    setArtistSongs([]);
    try {
      const res = await fetchWithTimeout(`http://localhost:8000/artist/${artistId}/songs`);
      if (!res.ok) return;
      const data = await res.json();
      setArtistSongs(data);
    } catch (err) {
      console.error("Artist songs fetch failed", err);
    }
  };

  const fetchAlbumTracks = async (recordingId: number) => {
    setAlbumLoading(true);
    setSelectedAlbum(null);
    try {
      const res = await fetchWithTimeout(`http://localhost:8000/recording/${recordingId}/album`);
      if (!res.ok) {
        setMessage("No album found for this recording.");
        setTimeout(() => setMessage(""), 3000);
        return;
      }
      const data = await res.json();
      setSelectedAlbum(data);
    } catch (err) {
      console.error("Album tracks fetch failed", err);
      setMessage("Error loading album tracks.");
      setTimeout(() => setMessage(""), 3000);
    } finally {
      setAlbumLoading(false);
    }
  };

  const fetchProfileTopTracks = async (profile: UserProfile) => {
    setSelectedProfile(profile);
    setProfileTopTracks([]);
    setProfileLoading(true);
    try {
      const res = await fetchWithTimeout(`http://localhost:8000/user/${profile.id}/top-tracks?n=5`);
      if (!res.ok) return;
      const data = await res.json();
      setProfileTopTracks(data.songs || []);
    } catch (err) {
      console.error("Profile top tracks fetch failed", err);
    } finally {
      setProfileLoading(false);
    }
  };

  // Fetch recommendations
  const fetchHome = async () => {
  if (!activeUser) return;
  setLoading(true);

  try {
    localStorage.removeItem(`home:${activeUser.id}`);
    const res = await fetch(`http://localhost:8000/recommend/${activeUser.id}?n=8&t=${Date.now()}`, { cache: "no-store" });
    const data = await res.json();
    setSongs(data.songs || []);
    setSource(data.source || "unknown");
  } catch (err) {
    setMessage("Error fetching home feed.");
    setSongs([]);
  } finally {
    setLoading(false);
  }
};
const playQueue = async (songList: Song[], startIndex: number = 0) => {
  if (!activeUser) return;
  const selected = songList[startIndex];
  if (!selected) return;
  setQueue([]);
  setQueueIndex(startIndex);
  playSong(selected);
};

const playingNextRef = useRef(false);
// Quando a música termina, busca a próxima do Redis
const playNext = async () => {
  if (playingNextRef.current) return;

  playingNextRef.current = true;

  try {
    if (!activeUser) return;

    const res = await fetch(
      `http://localhost:8000/queue/${activeUser.id}/next`
    );

    if (res.ok) {
      const song = await res.json();

      setQueueIndex(prev => prev + 1);
      setQueue(prev => prev.slice(1));
      playSong(song, false);
    } else {
      setIsPlaying(false);
      setQueue([]);
      setQueueIndex(0);
    }
  } finally {
    playingNextRef.current = false;
  }
};

  const playSong = async (song: Song, refreshQueue: boolean = true) => {
    if (!activeUser) return;
    setCurrentSong(song);
    setIsPlaying(true);
    setCurrentTime(0);
    setSelectedAlbum(null);
    try {
      await fetch("http://localhost:8000/event/play", {
        method: "POST",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify({ user_id: activeUser.id, recording_id: song.id, duration_ms: 30000 })
      });
      setMessage(`Playing "${song.name}" - Kafka event sent!`);
      if (refreshQueue) {
        const queueRes = await fetchWithTimeout(
          `http://localhost:8000/queue/${activeUser.id}/recommend-after/${song.id}?n=10`,
          { method: "POST" },
          60000
        );
        if (queueRes.ok) {
          const recommendedQueue = await queueRes.json();
          setQueue(Array.isArray(recommendedQueue) ? recommendedQueue : []);
          setQueueIndex(0);
        }
      }
      
      // Wait for Kafka -> Spark -> Redis processing before reading recs:{user_id}.
      // THEN refresh recommendations to see live update!
      setTimeout(() => {
        fetchHome();
        setMessage(`Recommendations updated by Live Spark Stream!`);
        setTimeout(() => setMessage(""), 3000);
      }, 5000);

    } catch (err) {
      setMessage("Error sending Kafka event.");
    }
  };

  const togglePlayback = () => {
    if (!currentSong) return;
    if (currentTime >= duration) {
      setCurrentTime(0);
      setIsPlaying(true);
      return;
    }
    setIsPlaying(prev => !prev);
  };

  const skipSong = async () => {
    if (!activeUser || !currentSong) return;
    try {
      await fetch("http://localhost:8000/event/skip", {
        method: "POST",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify({ user_id: activeUser.id, recording_id: currentSong.id, position_ms: currentTime * 1000 })
      });
      setMessage("Skip event sent to Kafka!");
      setTimeout(() => setMessage(""), 3000);
    } catch (err) {
      console.error("Skip event failed", err);
    }
    // Avança pela queue do Redis (igual ao fim da música)
    await playNext();
  };

  const likeSong = async () => {
    if (!activeUser || !currentSong) return;
    try {
      setLikedSongKeys(prev => new Set(prev).add(songKey(currentSong)));
      await fetch("http://localhost:8000/event/like", {
        method: "POST",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify({ user_id: activeUser.id, recording_id: currentSong.id })
      });
      setMessage("Like event sent to Kafka!");
      setTimeout(() => setMessage(""), 3000);
    } catch (err) {
      console.error("Like event failed", err);
    }
  };

  return (
    <div className="flex flex-col h-screen bg-black text-white font-sans overflow-hidden">
      
      {users.length === 0 && loading && (
        <div className="fixed inset-0 bg-black z-[200] flex flex-col items-center justify-center">
          <Disc3 className="text-green-500 h-16 w-16 animate-spin mb-4" />
          <h1 className="text-xl font-bold">Connecting to Big Data Cluster...</h1>
          <p className="text-gray-500 text-sm mt-2">Checking Hive, Redis and Kafka availability</p>
        </div>
      )}

      {/* Top Section: Sidebar + Content */}
      <div className="flex flex-1 overflow-hidden">
        {/* Sidebar */}
        <div className="w-64 bg-black flex flex-col pt-6 pb-2 px-4 h-full border-r border-gray-800 hidden md:flex">
        <div className="flex items-center mb-8 px-2 text-white font-bold text-xl tracking-tight">
          <Disc3 className="text-green-500 mr-3 h-8 w-8" /> 
          BigData Music
        </div>
        
        <div className="space-y-4 flex-grow">
          <div 
            onClick={() => handleTabChange('home')}
            className={`flex items-center cursor-pointer px-2 py-1 transition group ${activeTab === 'home' ? 'text-white' : 'text-gray-400 hover:text-white'}`}
          >
            <Home className="mr-4 h-6 w-6 transition" /> 
            <span className="font-semibold">Home</span>
          </div>
          <div 
            onClick={() => handleTabChange('search')}
            className={`flex items-center cursor-pointer px-2 py-1 transition group ${activeTab === 'search' ? 'text-white' : 'text-gray-400 hover:text-white'}`}
          >
            <Search className="mr-4 h-6 w-6 transition" /> 
            <span className="font-semibold">Search</span>
          </div>
          <div 
            onClick={() => handleTabChange('social')}
            className={`flex items-center cursor-pointer px-2 py-1 transition group ${activeTab === 'social' ? 'text-white' : 'text-gray-400 hover:text-white'}`}
          >
            <UserIcon className="mr-4 h-6 w-6 transition" /> 
            <span className="font-semibold">Social</span>
          </div>
          <div 
            onClick={() => handleTabChange('library')}
            className={`flex items-center cursor-pointer px-2 py-1 transition group ${activeTab === 'library' ? 'text-white' : 'text-gray-400 hover:text-white'}`}
          >
            <Library className="mr-4 h-6 w-6 transition" /> 
            <span className="font-semibold">Your Library</span>
          </div>
        </div>

        <div className="mt-auto border-t border-gray-800 pt-4 px-2">
          <p className="text-[10px] text-gray-500 mb-2 uppercase font-bold tracking-widest">Active Backend</p>
          <div className="flex items-center bg-white/5 rounded p-2 border border-white/10">
            <div className={`h-2 w-2 rounded-full mr-2 ${source.includes('hive') ? 'bg-orange-500' : 'bg-green-500'}`}></div>
            <span className="text-xs font-mono text-gray-300 truncate">{source || "Connecting..."}</span>
          </div>
        </div>
      </div>

      {/* Main Content */}
      <div className="flex-1 flex flex-col bg-gradient-to-b from-gray-900 to-black relative min-w-0">
        
        {/* Header */}
        <div className="h-16 shrink-0 flex items-center justify-between px-8 bg-black/40 z-10">
          <div className="flex items-center space-x-2">
            <button 
              onClick={handleBack}
              className="rounded-full w-8 h-8 flex items-center justify-center bg-black/60 text-white hover:bg-black/80 transition"
            >
               <ChevronLeft className="h-5 w-5" />
            </button>
            <button 
              onClick={handleForward}
              className="rounded-full w-8 h-8 flex items-center justify-center bg-black/60 text-white hover:bg-black/80 transition"
            >
               <ChevronRight className="h-5 w-5" />
            </button>
            
            {activeTab === 'search' && (
              <div className="ml-4 flex items-center bg-[#242424] rounded-full px-4 py-2 w-80 border border-transparent focus-within:border-white focus-within:bg-[#2a2a2a] transition-all group">
                <Search className="h-5 w-5 text-gray-400 mr-3 group-focus-within:text-white" />
                <input
                  type="text"
                  placeholder="What do you want to listen to?"
                  className="bg-transparent text-white w-full outline-none text-sm placeholder-gray-400 font-medium"
                  value={searchQuery}
                  onChange={(e) => setSearchQuery(e.target.value)}
                />
              </div>
            )}
            
            {isFetching && (
               <div className="ml-4 flex items-center space-x-2 bg-black/40 px-3 py-1 rounded-full border border-white/10 animate-pulse">
                 <div className="w-2 h-2 bg-green-500 rounded-full"></div>
                 <span className="text-[10px] uppercase font-bold tracking-widest text-gray-400">Syncing...</span>
               </div>
            )}
          </div>
          
          <button
            onClick={() => fetch("http://localhost:8000/broadcast/play-random", { method: "POST" })}
            className="flex items-center space-x-2 bg-green-600 hover:bg-green-500 px-3 py-1.5 rounded-full text-xs font-bold transition"
          >
            <Disc3 className="h-3.5 w-3.5" />
            <span>Broadcast</span>
          </button>
          <div className="flex items-center space-x-3 relative">
            {/* Sino de Notificações */}
            <div className="relative">
              <button
                onClick={() => { setIsNotifMenuOpen(!isNotifMenuOpen); setIsUserMenuOpen(false); setNotifSeen(true); }}
                className="relative flex items-center justify-center w-8 h-8 rounded-full bg-black hover:bg-[#282828] border border-gray-800 transition"
              >
                <Bell className="h-6 w-6 text-gray-300" />
                {userNotifications.length > 0 && !notifSeen && (
                  <span className="absolute -top-1 -right-0 w-3 h-3 bg-green-500 rounded-full text-[9px] font-bold text-black flex items-center justify-center">
                    {userNotifications.length > 1 ? ' ' : userNotifications.length}
                  </span>
                )}
              </button>

              {isNotifMenuOpen && (
                <>
                  <div className="fixed inset-0 z-40" onClick={() => setIsNotifMenuOpen(false)}></div>
                  <div className="absolute top-full right-0 mt-2 w-80 bg-[#282828] rounded shadow-2xl border border-white/10 z-50 overflow-hidden py-1 animate-in fade-in zoom-in-95 duration-100">
                    <div className="px-4 py-3 border-b border-white/5 flex items-center justify-between">
                      <p className="text-[10px] text-gray-500 uppercase font-bold tracking-widest">Notificações</p>
                      <span className="text-[10px] text-green-500 font-bold">{userNotifications.length} novas</span>
                    </div>

                    <div className="max-h-80 overflow-y-auto custom-scrollbar">
                      {userNotifications.length > 0 ? userNotifications.map((notif, i) => (
                        <div
                          key={i}
                          className="px-4 py-3 hover:bg-white/10 transition border-b border-white/5 cursor-pointer"
                          onClick={() => {
                            fetchArtistSongs(notif.artist_id, notif.artist_name);
                            setIsNotifMenuOpen(false);
                            handleTabChange('social');
                          }}
                        >
                          <div className="flex items-center space-x-3">
                            <div className="w-8 h-8 bg-blue-600/20 rounded-full flex items-center justify-center text-blue-400 font-bold text-xs shrink-0">
                              {notif.artist_name[0]}
                            </div>
                            <div className="flex-1 min-w-0">
                              <p className="text-xs font-bold text-white truncate">{notif.artist_name}</p>
                              <p className="text-[10px] text-gray-400 truncate">Novo lançamento: {notif.release_name}</p>
                              <p className="text-[9px] text-gray-600 mt-0.5">{notif.notified_at}</p>
                            </div>
                            <Mic2 className="h-3.5 w-3.5 text-blue-400 shrink-0" />
                          </div>
                        </div>
                      )) : (
                        <div className="px-4 py-8 text-center text-gray-600 text-xs italic">
                          Sem notificações de momento.
                        </div>
                      )}
                    </div>
                  </div>
                </>
              )}
            </div>
             {/* Profile Dropdown Toggle */}
             <button 
               onClick={() => {setIsUserMenuOpen(!isUserMenuOpen);  setIsNotifMenuOpen(false);}}
               className="flex items-center space-x-2 bg-black hover:bg-[#282828] p-1 pr-3 rounded-full border border-gray-800 transition cursor-pointer"
             >
               <div className="w-7 h-7 bg-gradient-to-tr from-green-600 to-green-400 rounded-full flex items-center justify-center shadow-inner">
                 <UserIcon className="h-4 w-4 text-black fill-black/20" />
               </div>
               <div className="flex flex-col items-start leading-none pr-1 max-w-[100px] overflow-hidden">
                 <span className="text-xs font-bold truncate w-full text-left" title={activeUser?.username || "Guest"}>{activeUser?.username || "Guest"}</span>
                 <span className="text-[9px] text-gray-400 uppercase tracking-tighter truncate w-full text-left" title={`${activeUser?.country} • ${activeUser?.age}y`}>{activeUser?.country} • {activeUser?.age}y</span>
               </div>
               <ChevronDown className={`h-3 w-3 text-gray-400 transition-transform ${isUserMenuOpen ? 'rotate-180' : ''}`} />
             </button>

             {/* Dropdown Menu */}
             {isUserMenuOpen && (
               <>
                 <div className="fixed inset-0 z-40" onClick={() => setIsUserMenuOpen(false)}></div>
                 <div className="absolute top-full right-0 mt-2 w-72 bg-[#282828] rounded shadow-2xl border border-white/10 z-50 overflow-hidden py-1 animate-in fade-in zoom-in-95 duration-100">
                   <div className="px-4 py-3 border-b border-white/5">
                     <p className="text-[10px] text-gray-500 uppercase font-bold tracking-widest mb-2">Switch Profile</p>
                     {activeUser && (
                       <div
                         className="flex items-center space-x-3 mb-1 min-w-0 cursor-pointer hover:bg-white/5 rounded p-2 -mx-2"
                         onClick={() => {
                           fetchProfileTopTracks(activeUser);
                           setIsUserMenuOpen(false);
                           handleTabChange('social');
                         }}
                       >
                          <div className="w-10 h-10 bg-green-500 rounded-full flex items-center justify-center text-black font-bold shrink-0">
                            {activeUser.username ? activeUser.username[0].toUpperCase() : "?"}
                          </div>
                          <div className="min-w-0 flex-1">
                            <p className="font-bold text-white text-sm truncate" title={activeUser.username || "Anonymous"}>{activeUser.username || "Anonymous"}</p>
                            <p className="text-xs text-gray-400">ID: {activeUser.id} • top tracks</p>
                          </div>
                       </div>
                     )}
                   </div>
                   
                   <div className="max-h-60 overflow-y-auto py-1">
                     {users.map(u => (
                       <button 
                         key={u.id}
                         onClick={() => { setActiveUser(u); setIsUserMenuOpen(false); }}
                         className={`w-full text-left px-4 py-3 hover:bg-white/10 flex items-center justify-between group transition min-w-0 ${activeUser?.id === u.id ? 'bg-white/5' : ''}`}
                       >
                         <div className="flex items-center space-x-3 min-w-0 flex-1">
                           <div className="w-8 h-8 bg-gray-700 rounded-full flex items-center justify-center text-xs font-bold text-gray-300 shrink-0">
                             {u.username ? u.username[0].toUpperCase() : "?"}
                           </div>
                           <div className="flex flex-col min-w-0 flex-1">
                             <span className={`text-sm font-semibold truncate ${activeUser?.id === u.id ? 'text-green-500' : 'text-gray-200'}`} title={u.username || "Anonymous"}>{u.username || "Anonymous"}</span>
                             <div className="flex items-center text-[10px] text-gray-500 space-x-2">
                               <span className="flex items-center"><Globe className="h-2.5 w-2.5 mr-1" /> {u.country}</span>
                               <span className="flex items-center"><Calendar className="h-2.5 w-2.5 mr-1" /> {u.age}y</span>
                             </div>
                           </div>
                         </div>
                         {activeUser?.id === u.id && <div className="w-1.5 h-1.5 bg-green-500 rounded-full"></div>}
                       </button>
                     ))}
                   </div>
                   
                   <div className="p-3 border-t border-white/5 bg-black/20">
                     <input 
                       type="number" 
                       placeholder="Enter manual ID..." 
                       className="bg-white/5 text-white w-full rounded px-3 py-2 outline-none text-xs border border-white/10 focus:border-green-500 transition"
                       onKeyDown={(e) => {
                         if (e.key === 'Enter') {
                           setActiveUser({ id: Number(e.currentTarget.value), username: "Manual", age: 0, country: "???" });
                           setIsUserMenuOpen(false);
                         }
                       }}
                     />
                   </div>
                 </div>
               </>
             )}
          </div>
        </div>

        {/* Content Area */}
        <div className="flex-1 overflow-y-auto p-8 pb-8 custom-scrollbar">
          {message && (
            <div className="bg-blue-600/80 backdrop-blur text-white p-3 rounded-lg mb-6 flex items-center shadow-lg border border-blue-400 text-sm font-medium">
              <Info className="h-5 w-5 mr-3" />
              {message}
            </div>
          )}

          {activeTab === 'home' && (
            <>
              {/* Mixes as Playlists */}
              <div className="mb-12">
                <h3 className="text-2xl font-bold mb-6">Made For You</h3>
                <div className="grid grid-cols-2 md:grid-cols-3 gap-6">
                  {['morning', 'afternoon', 'night'].map((mix) => (
                    <div 
                      key={mix}
                      onClick={() => { setTimeOfDay(mix); handleTabChange('library'); }}
                      className="bg-gradient-to-br from-green-900/40 to-black p-6 rounded-xl border border-white/5 hover:border-green-500/50 transition cursor-pointer group"
                    >
                      <div className="w-full aspect-square bg-gray-800 rounded-lg mb-4 flex items-center justify-center relative overflow-hidden shadow-2xl">
                        <Disc3 className={`h-24 w-24 ${mix === 'morning' ? 'text-yellow-500' : mix === 'afternoon' ? 'text-orange-500' : 'text-blue-500'} opacity-40`} />
                        <Play className="h-12 w-12 text-white absolute opacity-0 group-hover:opacity-100 transition-opacity" />
                      </div>
                      <h4 className="text-xl font-bold capitalize">{mix} Mix</h4>
                      <p className="text-gray-400 text-sm">Your daily dose of {mix} vibes.</p>
                    </div>
                  ))}
                </div>
              </div>

              <div className="mb-12">
                <div className="flex items-center justify-between mb-6">
                  <h3 className="text-xl font-bold">Recommended for You</h3>
                  <span className="text-xs text-gray-500 font-mono">Source: {source}</span>
                </div>
                {loading ? (
                   <div className="flex justify-center items-center h-48">
                     <div className="animate-spin rounded-full h-10 w-10 border-t-2 border-b-2 border-green-500"></div>
                   </div>
                ) : (
                  <div className="grid grid-cols-2 sm:grid-cols-3 lg:grid-cols-4 xl:grid-cols-5 gap-6">
                  {songs.length > 0 ? (
                    songs.map((song, index) => (
                      <div 
                        key={index} 
                        className="bg-[#181818] p-4 rounded hover:bg-[#282828] transition duration-300 group cursor-pointer relative"
                        onClick={() => playQueue(songs, index)}
                      >
                        <div className="w-full aspect-square bg-gray-800 rounded mb-4 shadow-2xl flex items-center justify-center relative overflow-hidden">
                          <Music className="h-16 w-16 text-gray-500" />
                          <div className={`absolute bottom-2 right-2 bg-green-500 rounded-full p-3 shadow-xl transition-all duration-300 flex items-center justify-center 
                            ${isCurrentSong(song) 
                              ? 'opacity-100 translate-y-0' 
                              : 'opacity-0 translate-y-2 group-hover:opacity-100 group-hover:translate-y-0'
                            } hover:scale-105 hover:bg-green-400`}
                          >
                            {isCurrentSong(song) && isPlaying ? (
                              <Pause className="h-6 w-6 text-black fill-black" />
                            ) : (
                              <Play className="h-6 w-6 text-black fill-black ml-1" />
                            )}
                          </div>
                        </div>
                        <h3 className="font-bold text-white truncate text-base mb-1" title={song.name}>{song.name}</h3>
                        <p className="text-gray-400 text-xs truncate font-medium" title={song.artist}>{song.artist}</p>
                      </div>
                    ))
                  ) : isFetching ? (
                    Array.from({ length: 10 }).map((_, i) => (
                      <div key={i} className="bg-[#181818]/50 p-4 rounded animate-pulse">
                        <div className="w-full aspect-square bg-white/5 rounded mb-4"></div>
                        <div className="h-4 bg-white/10 rounded w-3/4 mb-2"></div>
                        <div className="h-3 bg-white/5 rounded w-1/2"></div>
                      </div>
                    ))
                  ) : (
                    <div className="col-span-full py-10 flex justify-center text-gray-500 text-sm italic">
                      No recommendations available.
                    </div>
                  )}
                  </div>
                )}
              </div>
            </>
          )}

          {activeTab === 'search' && (
             <div className="flex flex-col h-full mt-4">
                {searchQuery ? (
                  <>
                    <h2 className="text-2xl font-bold mb-6">Search results for "{searchQuery}"</h2>
                    <div className="grid grid-cols-2 sm:grid-cols-3 lg:grid-cols-4 xl:grid-cols-5 gap-6 mb-12">
                      {searchResults.length > 0 ? searchResults.map((song, index) => (
                        <div 
                          key={index} 
                          className="bg-[#181818] p-4 rounded hover:bg-[#282828] transition duration-300 group cursor-pointer relative"
                          onClick={() => playQueue(searchResults, index)}
                        >
                          <div className="w-full aspect-square bg-gray-800 rounded mb-4 shadow-2xl flex items-center justify-center relative overflow-hidden">
                            <Music className="h-16 w-16 text-gray-500" />
                            <div className={`absolute bottom-2 right-2 bg-green-500 rounded-full p-3 shadow-xl transition-all duration-300 flex items-center justify-center 
                              ${isCurrentSong(song) 
                                ? 'opacity-100 translate-y-0' 
                                : 'opacity-0 translate-y-2 group-hover:opacity-100 group-hover:translate-y-0'
                              } hover:scale-105 hover:bg-green-400`}
                            >
                              {isCurrentSong(song) && isPlaying ? (
                                <Pause className="h-6 w-6 text-black fill-black" />
                              ) : (
                                <Play className="h-6 w-6 text-black fill-black ml-1" />
                              )}
                            </div>
                            {song.score && (
                              <div className="absolute top-2 right-2 bg-black/60 backdrop-blur-sm text-[10px] px-2 py-0.5 rounded text-gray-300 font-mono">
                                Score: {song.score.toFixed(1)}
                              </div>
                            )}
                          </div>
                          <h3 className="font-bold text-white truncate text-base mb-1" title={song.name}>{song.name}</h3>
                          <p className="text-gray-400 text-xs truncate font-medium" title={song.artist}>{song.artist}</p>
                        </div>
                      )) : (
                        <p className="text-gray-500 italic col-span-full py-10 text-center">No tracks found. Try searching for "radiohead" or "queen".</p>
                      )}
                    </div>
                  </>
                ) : (
                  <>
                    <h2 className="text-2xl font-bold mb-6">Browse all</h2>
                    <div className="grid grid-cols-2 md:grid-cols-3 lg:grid-cols-4 xl:grid-cols-5 gap-6">
                      {['Pop', 'Hip-Hop', 'Rock', 'Electronic', 'Jazz', 'Classical', 'Big Data', 'Streaming'].map((genre, i) => (
                        <div key={i} className="h-44 rounded-lg p-4 font-bold text-xl cursor-pointer relative overflow-hidden transition-transform hover:scale-[1.02]" style={{backgroundColor: `hsl(${i * 45}, 60%, 35%)`}}>
                          {genre}
                          <Music className="h-20 w-20 text-white/10 absolute -bottom-4 -right-4 rotate-12" />
                        </div>
                      ))}
                    </div>
                  </>
                )}
             </div>
          )}

          {activeTab === 'social' && (
            <div className="space-y-12">
              <h2 className="text-3xl font-bold tracking-tight">Social Network</h2>
              
              <div className="grid grid-cols-1 lg:grid-cols-3 gap-8">
                {/* Your Connections */}
                <div className="bg-white/5 rounded-2xl p-6 border border-white/5">
                  <h3 className="text-xl font-bold mb-6 flex items-center">
                    <UserIcon className="h-5 w-5 mr-3 text-green-500" />
                    Your Connections
                  </h3>
                  <div className="space-y-4 max-h-[400px] overflow-y-auto pr-2 custom-scrollbar">
                    {userFriends.length > 0 ? userFriends.map(u => (
                      <div
                        key={u.id}
                        className={`flex items-center space-x-4 p-3 rounded-xl border transition group cursor-pointer ${selectedProfile?.id === u.id ? 'bg-green-600/20 border-green-500/50' : 'bg-white/5 border-white/5 hover:bg-white/10'}`}
                        onClick={() => fetchProfileTopTracks(u)}
                      >
                        <div className="w-12 h-12 bg-gray-700 rounded-full flex items-center justify-center font-bold text-gray-300 shrink-0">
                          {u.username ? u.username[0].toUpperCase() : "?"}
                        </div>
                        <div className="min-w-0 flex-1">
                          <p className="font-bold text-sm truncate" title={u.username}>{u.username}</p>
                          <p className="text-[10px] text-gray-500 truncate" title={`${u.country} • ${u.age}y`}>{u.country} • {u.age}y</p>
                        </div>
                      </div>
                    )) : isFetching ? (
                      Array.from({ length: 5 }).map((_, i) => (
                        <div key={i} className="flex items-center space-x-4 p-3 bg-white/5 rounded-xl border border-white/5 animate-pulse">
                          <div className="w-12 h-12 bg-white/10 rounded-full"></div>
                          <div className="flex-1 space-y-2">
                            <div className="h-3 bg-white/10 rounded w-1/2"></div>
                            <div className="h-2 bg-white/5 rounded w-1/3"></div>
                          </div>
                        </div>
                      ))
                    ) : (
                      <p className="text-gray-600 text-xs italic">You haven't added any friends yet.</p>
                    )}
                  </div>
                  {selectedProfile && (
                    <div className="mt-6 pt-5 border-t border-white/10">
                      <h4 className="text-xs font-bold uppercase tracking-widest text-green-400 mb-3">
                        Top 5 from {selectedProfile.username}
                      </h4>
                      <div className="space-y-2">
                        {profileTopTracks.length > 0 ? profileTopTracks.map((song, i) => (
                          <div
                            key={`${song.id}-${i}`}
                            className="flex items-center justify-between p-2 hover:bg-white/5 rounded cursor-pointer group"
                            onClick={() => playQueue(profileTopTracks, i)}
                          >
                            <span className="text-xs text-gray-400 w-5">{i + 1}</span>
                            <div className="min-w-0 flex-1">
                              <p className="text-xs font-semibold truncate group-hover:text-green-400" title={song.name}>{song.name}</p>
                              <p className="text-[10px] text-gray-500 truncate" title={song.artist}>{song.artist}</p>
                            </div>
                            <span className="text-[10px] text-gray-500 ml-2">{song.score?.toFixed(0)}</span>
                          </div>
                        )) : (
                          <p className="text-xs text-gray-600 italic">{profileLoading ? "Loading..." : "No plays found."}</p>
                        )}
                      </div>
                    </div>
                  )}
                </div>

                {/* Friends Activity */}
                <div className="bg-white/5 rounded-2xl p-6 border border-white/5">
                  <h3 className="text-xl font-bold mb-6 flex items-center">
                    <Music className="h-5 w-5 mr-3 text-green-500" />
                    Friends Activity
                  </h3>
                  <div className="space-y-4 max-h-[400px] overflow-y-auto pr-2 custom-scrollbar">
                    {friendHistory.length > 0 ? friendHistory.map((song, i) => (
                      <div key={i} className="flex items-center justify-between group p-3 hover:bg-white/10 rounded-xl bg-white/5 border border-white/5 transition cursor-pointer" onClick={() => playQueue(friendHistory, i)}>
                        <div className="flex items-center min-w-0">
                          <div className="w-10 h-10 bg-gray-800 rounded flex items-center justify-center mr-4 shrink-0">
                            <Music className="h-4 w-4 text-gray-500" />
                          </div>
                          <div className="truncate min-w-0 flex-1">
                            <p className="text-sm font-semibold truncate text-gray-200 group-hover:text-white" title={song.name}>{song.name}</p>
                            <p className="text-[10px] text-gray-500 truncate" title={song.artist}>{song.artist}</p>
                          </div>
                        </div>
                        <div className="flex flex-col items-end shrink-0 ml-2">
                           <span className="text-xs font-bold text-green-500">{song.score} plays</span>
                           <span className="text-[9px] text-gray-500">by friends</span>
                        </div>
                      </div>
                    )) : isFetching ? (
                      Array.from({ length: 5 }).map((_, i) => (
                        <div key={i} className="flex items-center justify-between p-3 rounded-xl bg-white/5 border border-white/5 animate-pulse">
                          <div className="flex items-center min-w-0 w-full">
                            <div className="w-10 h-10 bg-white/10 rounded mr-4 shrink-0"></div>
                            <div className="flex-1 space-y-2">
                              <div className="h-3 bg-white/10 rounded w-2/3"></div>
                              <div className="h-2 bg-white/5 rounded w-1/3"></div>
                            </div>
                          </div>
                        </div>
                      ))
                    ) : (
                      <p className="text-gray-600 text-xs italic">Your friends are quiet today...</p>
                    )}
                  </div>
                </div>

                {/* Artist Feed */}
                <div className="bg-white/5 rounded-2xl p-6 border border-white/5 overflow-hidden">
                  <h3 className="text-xl font-bold mb-6 flex items-center">
                    <Mic2 className="h-5 w-5 mr-3 text-blue-500" />
                    Following Artists
                  </h3>
                  
                  <div className="flex flex-col space-y-6">
                    <div className="space-y-4 max-h-[400px] overflow-y-auto pr-2 custom-scrollbar">
                      {userNotifications.length > 0 ? userNotifications.map((notif, i) => (
                        <div 
                          key={i} 
                          className={`flex items-center p-3 rounded-lg border transition cursor-pointer ${selectedArtist === notif.artist_name ? 'bg-blue-600/20 border-blue-500/50' : 'bg-white/5 border-white/5 hover:bg-white/10'}`}
                          onClick={() => fetchArtistSongs(notif.artist_id, notif.artist_name)}
                        >
                          <div className="w-10 h-10 bg-blue-600/20 rounded-full flex items-center justify-center mr-4 text-blue-400 font-bold text-xs shrink-0">
                            {notif.artist_name[0]}
                          </div>
                          <div className="flex-1 min-w-0 pr-2">
                            <p className="text-sm font-bold text-gray-100 truncate" title={notif.artist_name}>{notif.artist_name}</p>
                            <p className="text-[10px] text-gray-400 truncate" title={`Recently released: ${notif.release_name}`}>Recently released: {notif.release_name}</p>
                          </div>
                          <ChevronRight className={`h-4 w-4 text-gray-600 transition-transform shrink-0 ${selectedArtist === notif.artist_name ? 'rotate-90 text-blue-400' : ''}`} />
                        </div>
                      )) : isFetching ? (
                        Array.from({ length: 5 }).map((_, i) => (
                          <div key={i} className="flex items-center p-3 rounded-lg border border-white/5 animate-pulse">
                            <div className="w-10 h-10 bg-white/10 rounded-full mr-4 shrink-0"></div>
                            <div className="flex-1 space-y-2">
                              <div className="h-3 bg-white/10 rounded w-3/4"></div>
                              <div className="h-2 bg-white/5 rounded w-1/2"></div>
                            </div>
                          </div>
                        ))
                      ) : (
                        <p className="text-gray-600 text-xs italic">No artist updates in your feed.</p>
                      )}
                    </div>

                    {/* Popular Songs of Selected Artist */}
                    {selectedArtist && (
                      <div className="pt-6 border-t border-white/10 animate-in slide-in-from-bottom-4 duration-300">
                        <div className="flex items-center justify-between mb-4">
                          <h4 className="text-sm font-bold uppercase tracking-widest text-blue-400">Popular from {selectedArtist}</h4>
                          <span className="text-[10px] text-gray-500 font-mono">Sorted by plays</span>
                        </div>
                        <div className="space-y-2">
                          {artistSongs.length > 0 ? artistSongs.map((song) => (
                            <div 
                              key={song.id} 
                              className="flex items-center justify-between p-2 hover:bg-white/5 rounded transition group cursor-pointer"
                              onClick={() =>
                                playQueue(
                                  artistSongs,
                                  artistSongs.findIndex(s => s.id === song.id)
                                )
                              }
                            >
                              <div className="flex items-center space-x-3 min-w-0 flex-1">
                                <div className="w-6 h-6 bg-gray-800 rounded flex items-center justify-center group-hover:bg-green-500 transition shrink-0">
                                  <Play className="h-3 w-3 text-white fill-white ml-0.5" />
                                </div>
                                <span className="text-xs font-medium truncate" title={song.name}>{song.name}</span>
                              </div>
                              <span className="text-[10px] text-gray-500 font-mono whitespace-nowrap ml-2 shrink-0">{song.score?.toFixed(0)} plays</span>
                            </div>
                          )) : (
                            <div className="flex justify-center py-4">
                              <div className="animate-pulse text-xs text-gray-600">Loading artist catalog...</div>
                            </div>
                          )}
                        </div>
                      </div>
                    )}
                  </div>
                </div>
              </div>
            </div>
          )}

          {activeTab === 'library' && (
            <div className="space-y-8">
              <h2 className="text-3xl font-bold tracking-tight">Your Library</h2>
              
              <div className="grid grid-cols-1 xl:grid-cols-4 gap-8">
                {/* Playlist Sidebar */}
                <div className="xl:col-span-1 space-y-4">
                  <div className="bg-green-600/20 p-6 rounded-2xl border border-green-500/20">
                    <h3 className="font-bold text-xl mb-2 flex items-center capitalize"><Disc3 className="mr-2 h-5 w-5" /> {timeOfDay} Mix</h3>
                    <p className="text-xs text-gray-400 mb-4">Curated by Big Data engine for your {timeOfDay} listening habits.</p>
                   <button
                      className="w-full bg-green-500 text-black font-bold py-2 rounded-full hover:scale-105 transition shadow-lg"
                      onClick={() => playlist.length > 0 && playQueue(playlist, 0)}
                    >
                      PLAY MIX
                    </button>
                  </div>
                  
                  <div className="bg-white/5 p-4 rounded-xl border border-white/5">
                    <p className="text-[10px] text-gray-500 uppercase font-bold tracking-widest mb-3">Saved Playlists</p>
                    {['Liked Songs', 'Daily Drive', 'Discover Weekly'].map(p => (
                      <div key={p} className="py-2 px-3 hover:bg-white/5 rounded-lg cursor-pointer text-sm font-medium text-gray-300 hover:text-white transition">
                        {p}
                      </div>
                    ))}
                  </div>
                </div>

                {/* Playlist Content */}
                <div className="xl:col-span-3">
                  <div className="bg-black/20 rounded-2xl overflow-hidden border border-white/5">
                    <table className="w-full text-left border-collapse table-fixed">
                      <thead className="bg-white/5 text-[10px] text-gray-500 uppercase tracking-widest border-b border-white/5">
                        <tr>
                          <th className="px-6 py-4 font-bold w-16">#</th>
                          <th className="px-6 py-4 font-bold w-5/12">Title</th>
                          <th className="px-6 py-4 font-bold w-4/12">Artist</th>
                          <th className="px-6 py-4 font-bold w-2/12">Plays</th>
                        </tr>
                      </thead>
                      <tbody className="divide-y divide-white/5">
                        {playlist.length > 0 ? playlist.map((song, i) => (
                          <tr
                            key={song.id}
                            className="hover:bg-white/5 group transition cursor-pointer"
                            onClick={() => playQueue(playlist, i)}
                          >
                            <td className="px-6 py-4 text-gray-500 text-xs">{i + 1}</td>
                            <td className="px-6 py-4 min-w-0">
                              <div className="flex items-center min-w-0">
                                <div className="w-8 h-8 bg-gray-800 rounded flex items-center justify-center mr-3 shrink-0">
                                  <Music className="h-3 w-3 text-gray-600" />
                                </div>
                                <span className="text-sm font-semibold group-hover:text-green-500 transition truncate block" title={song.name}>{song.name}</span>
                              </div>
                            </td>
                            <td className="px-6 py-4 text-sm text-gray-400 truncate" title={song.artist}>{song.artist}</td>
                            <td className="px-6 py-4 text-xs font-mono text-gray-500 truncate" title={song.score?.toFixed(0)}>{song.score?.toFixed(0)}</td>
                          </tr>
                        )) : isFetching ? (
                          Array.from({ length: 10 }).map((_, i) => (
                            <tr key={i} className="animate-pulse">
                              <td className="px-6 py-4"><div className="h-3 bg-white/10 rounded w-4"></div></td>
                              <td className="px-6 py-4">
                                <div className="flex items-center">
                                  <div className="w-8 h-8 bg-white/10 rounded mr-3"></div>
                                  <div className="h-3 bg-white/5 rounded w-32"></div>
                                </div>
                              </td>
                              <td className="px-6 py-4"><div className="h-3 bg-white/10 rounded w-24"></div></td>
                              <td className="px-6 py-4"><div className="h-3 bg-white/5 rounded w-8"></div></td>
                            </tr>
                          ))
                        ) : (
                          <tr><td colSpan={4} className="px-6 py-10 text-center text-gray-600 italic text-sm">No songs found in this mix.</td></tr>
                        )}
                      </tbody>
                    </table>
                  </div>
                </div>
              </div>
            </div>
          )}
        </div>
      </div>
    </div>

      {(selectedAlbum || albumLoading) && (
        <div className="absolute bottom-24 left-4 right-4 md:left-72 md:right-8 z-40 bg-[#181818] border border-white/10 rounded-lg shadow-2xl max-h-80 overflow-hidden">
          <div className="flex items-center justify-between px-4 py-3 border-b border-white/10">
            <div className="min-w-0">
              <p className="text-[10px] uppercase tracking-widest text-gray-500 font-bold">Album Tracks</p>
              <h3 className="font-bold text-sm truncate">{selectedAlbum?.release_name || "Loading..."}</h3>
            </div>
            <button className="text-gray-400 hover:text-white text-sm px-2" onClick={() => setSelectedAlbum(null)}>
              Close
            </button>
          </div>
          <div className="overflow-y-auto custom-scrollbar max-h-64">
            {albumLoading ? (
              <div className="px-4 py-6 text-sm text-gray-500">Loading tracks...</div>
            ) : selectedAlbum?.tracks.length ? selectedAlbum.tracks.map((song, i) => (
              <div
                key={`${song.id}-${i}`}
                className="flex items-center px-4 py-2 hover:bg-white/5 cursor-pointer group"
                onClick={() => playQueue(selectedAlbum.tracks, i)}
              >
                <span className="text-xs text-gray-500 w-10">{song.number || song.position || i + 1}</span>
                <div className="min-w-0 flex-1">
                  <p className="text-sm font-semibold truncate group-hover:text-green-400" title={song.name}>{song.name}</p>
                  <p className="text-[10px] text-gray-500 truncate" title={song.artist}>{song.artist}</p>
                </div>
              </div>
            )) : (
              <div className="px-4 py-6 text-sm text-gray-500">No tracks found.</div>
            )}
          </div>
        </div>
      )}

      {/* Player Bar */}
      <div className="h-24 shrink-0 bg-black border-t border-white/5 flex items-center px-4 justify-between z-50">
        <div className="flex items-center w-1/3 min-w-0 pr-4">
          {currentSong ? (
            (() => {
              const isLiked = likedSongKeys.has(songKey(currentSong));
              return (
            <>
              <button
                className="w-14 h-14 bg-gray-800 rounded shadow-md flex items-center justify-center flex-shrink-0 hover:bg-gray-700 transition"
                onClick={() => fetchAlbumTracks(currentSong.id)}
                title="Show album tracks"
              >
                 <Music className="h-6 w-6 text-gray-500" />
              </button>
              <div className="ml-4 truncate min-w-0 flex-1">
                <p className="text-white text-sm font-semibold truncate hover:underline cursor-pointer" title={currentSong.name}>{currentSong.name}</p>
                <p className="text-gray-400 text-[11px] truncate hover:underline cursor-pointer" title={currentSong.artist}>{currentSong.artist}</p>
              </div>
              <Heart 
                className={`h-4 w-4 ml-4 cursor-pointer flex-shrink-0 transition hover:scale-110 ${isLiked ? 'text-green-500 fill-green-500' : 'text-gray-400 hover:text-white'}`}
                onClick={() => currentSong && likeSong()}
              />
            </>
              );
            })()
          ) : (
             <p className="text-gray-500 text-xs italic">Choose music from Hive cluster...</p>
          )}
        </div>

        <div className="flex flex-col items-center w-1/3 max-md:w-1/2">
          <div className="flex items-center space-x-6 mb-2">
            <SkipBack className="h-5 w-5 text-gray-400 cursor-not-allowed" />
            <button 
              className="bg-white text-black rounded-full w-8 h-8 flex items-center justify-center hover:scale-105 transition"
              onClick={togglePlayback}
            >
               {isPlaying ? <Pause className="h-4 w-4 fill-black" /> : <Play className="h-4 w-4 fill-black ml-0.5" />}
            </button>
            <SkipForward
              className={`h-5 w-5 transition ${currentSong ? 'text-white cursor-pointer hover:scale-110' : 'text-gray-600 cursor-not-allowed'}`}
              onClick={() => currentSong && skipSong()}
            />
          </div>
          <div className="w-full flex items-center space-x-2">
            <span className="text-[10px] text-gray-500 font-mono">{formatTime(currentTime)}</span>
            <div className="flex-1 h-1 bg-white/10 rounded-full overflow-hidden group hover:h-1.5 transition-all">
              <div 
                className="h-full bg-white group-hover:bg-green-500 transition-colors transition-all duration-1000 ease-linear"
                style={{ width: `${(currentTime / duration) * 100}%` }}
              ></div>
            </div>
            <span className="text-[10px] text-gray-500 font-mono">{formatTime(duration)}</span>
          </div>
        </div>

        <div className="w-1/3 flex justify-end items-center space-x-3 text-gray-400 min-w-0">
           {queue.length > 0 && (
             <div className="hidden lg:block w-48 min-w-0">
               <p className="text-[10px] uppercase tracking-widest text-gray-500 font-bold mb-1">Up Next</p>
               <div className="space-y-0.5">
                 {queue.slice(0, 3).map((song, i) => (
                   <p key={`${song.id}-${i}`} className="text-[10px] truncate text-gray-300" title={`${song.name} - ${song.artist}`}>
                     {i + 1}. {song.name}
                   </p>
                 ))}
               </div>
             </div>
           )}
           <Mic2 className="h-4 w-4 shrink-0" />
           <Volume2 className="h-5 w-5 shrink-0" />
           <div className="w-24 h-1 bg-white/20 rounded-full shrink-0">
             <div className="h-full bg-white rounded-full w-2/3"></div>
           </div>
        </div>
      </div>

      {/* Notifications Container */}
      <div className="fixed top-20 right-8 z-[100] space-y-3 pointer-events-none">
        {notifications.map(n => (
          <div key={n.id} className="bg-blue-600/90 backdrop-blur-md text-white px-6 py-4 rounded-xl shadow-2xl border border-blue-400/50 flex items-center animate-in slide-in-from-right-10 duration-300 pointer-events-auto min-w-[300px]">
            <div className="bg-blue-400/30 rounded-full p-2 mr-4">
              <Info className="h-5 w-5 text-blue-100" />
            </div>
            <div className="flex flex-col">
              <span className="text-[10px] uppercase tracking-widest text-blue-200 font-bold mb-0.5">Real-time Stream</span>
              <span className="text-sm font-semibold">{n.message}</span>
            </div>
          </div>
        ))}
      </div>
      
    </div>
  );
}

export default App;
