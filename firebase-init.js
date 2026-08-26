  import { initializeApp } from "https://www.gstatic.com/firebasejs/10.12.1/firebase-app.js";
  import { getAuth, GoogleAuthProvider, signInWithPopup, signInWithRedirect, getRedirectResult, signOut, RecaptchaVerifier, signInWithPhoneNumber, FacebookAuthProvider, signInWithCredential } from "https://www.gstatic.com/firebasejs/10.12.1/firebase-auth.js";
  import { getFirestore, doc, getDoc, setDoc, collection, getDocs, deleteDoc, onSnapshot, writeBatch, addDoc, query, orderBy, limit, where } from "https://www.gstatic.com/firebasejs/10.12.1/firebase-firestore.js";

  const firebaseConfig = {
    apiKey: "AIzaSyCMmrM70Q51rXHfutFYj2fZqC3XETEFT6c",
    authDomain: "rishabh-b2ead.firebaseapp.com",
    projectId: "rishabh-b2ead",
    storageBucket: "rishabh-b2ead.firebasestorage.app",
    messagingSenderId: "548653522442",
    appId: "1:548653522442:web:6754ba2fc0421c7ca1c9f3"
  };

  const app = initializeApp(firebaseConfig);
  const auth = getAuth(app);
  const db   = getFirestore(app);
  window.db = db;
  window._fbAddDoc = addDoc;
  window._fbCollection = collection;
  window._fbQuery = query;
  window._fbOrderBy = orderBy;
  window._fbLimit = limit;
  window._fbGetDocs = getDocs;
  window._fbDeleteDoc = deleteDoc;
  window._fbDoc = doc;
  window._fbSetDoc = setDoc;
  window._fbGetDoc = getDoc;
  const googleProvider = new GoogleAuthProvider();
  googleProvider.setCustomParameters({ prompt: 'select_account' });

  /* ── Mobile check ── */
  const isMobile = /Android|iPhone|iPad|iPod/i.test(navigator.userAgent);

  /* ══════════════════════════════════════════════
     FIRESTORE DB HELPER — window.FSDB
     Only 3 collections in the cloud:
       users        → ff_users/{email}
       cardRegistry → ff_cards/{cardId}
       cardBin      → ff_bin/{cardId}
  ══════════════════════════════════════════════ */
  window.FSDB = {

    /* ── USERS ── */
    async saveUser(email, userData){
      try{
        const key = email.replace(/\./g,'_');
        await setDoc(doc(db,'ff_users',key), userData, {merge:true});
      }catch(e){ console.warn('FSDB.saveUser error:',e); }
    },

    async loadUser(email){
      try{
        const key = email.replace(/\./g,'_');
        const snap = await getDoc(doc(db,'ff_users',key));
        return snap.exists() ? snap.data() : null;
      }catch(e){ console.warn('FSDB.loadUser error:',e); return null; }
    },

    async loadAllUsers(){
      try{
        const snap = await getDocs(collection(db,'ff_users'));
        const result = {};
        snap.forEach(d => { const u=d.data(); if(u.email) result[u.email]=u; });
        return result;
      }catch(e){ console.warn('FSDB.loadAllUsers error:',e); return {}; }
    },

    async deleteUser(email){
      try{
        const key = email.replace(/\./g,'_');
        await deleteDoc(doc(db,'ff_users',key));
      }catch(e){ console.warn('FSDB.deleteUser error:',e); }
    },

    /* ── CARD REGISTRY ── */
    async saveCard(card){
      try{
        await setDoc(doc(db,'ff_cards',card.id), card);
      }catch(e){ console.warn('FSDB.saveCard error:',e); }
    },

    async saveAllCards(cards){
      try{
        /* Batch write — max 500 per batch */
        const chunks = [];
        for(let i=0;i<cards.length;i+=400) chunks.push(cards.slice(i,i+400));
        for(const chunk of chunks){
          const batch = writeBatch(db);
          chunk.forEach(c => batch.set(doc(db,'ff_cards',c.id), c));
          await batch.commit();
        }
      }catch(e){ console.warn('FSDB.saveAllCards error:',e); }
    },

    async loadAllCards(){
      try{
        const snap = await getDocs(collection(db,'ff_cards'));
        const result = [];
        snap.forEach(d => result.push(d.data()));
        return result;
      }catch(e){ console.warn('FSDB.loadAllCards error:',e); return []; }
    },

    async deleteCard(cardId){
      try{
        await deleteDoc(doc(db,'ff_cards',cardId));
      }catch(e){ console.warn('FSDB.deleteCard error:',e); }
    },

    /* ── CARD BIN ── */
    async saveBin(cards){
      try{
        const chunks = [];
        for(let i=0;i<cards.length;i+=400) chunks.push(cards.slice(i,i+400));
        for(const chunk of chunks){
          const batch = writeBatch(db);
          chunk.forEach(c => batch.set(doc(db,'ff_bin',c.id), c));
          await batch.commit();
        }
      }catch(e){ console.warn('FSDB.saveBin error:',e); }
    },

    async loadBin(){
      try{
        const snap = await getDocs(collection(db,'ff_bin'));
        const result = [];
        snap.forEach(d => result.push(d.data()));
        return result;
      }catch(e){ console.warn('FSDB.loadBin error:',e); return []; }
    },

    async deleteFromBin(cardId){
      try{
        await deleteDoc(doc(db,'ff_bin',cardId));
      }catch(e){ console.warn('FSDB.deleteFromBin error:',e); }
    },

    /* ── LIVE CARDS LISTENER — realtime nearby-map updates ── */
    _cardsUnsub: null,
    listenCards(onChange){
      try{
        if(this._cardsUnsub) this._cardsUnsub();
        this._cardsUnsub = onSnapshot(collection(db,'ff_cards'), function(snap){
          var cards = [];
          snap.forEach(function(d){ cards.push(d.data()); });
          try{ localStorage.setItem('ff_cardRegistry', JSON.stringify(cards)); }catch(e){}
          if(typeof onChange==='function') onChange(cards);
        }, function(err){ console.warn('FSDB.listenCards error:',err); });
      }catch(e){ console.warn('FSDB.listenCards setup error:',e); }
    },
    stopListenCards(){
      if(this._cardsUnsub){ try{ this._cardsUnsub(); }catch(e){} this._cardsUnsub=null; }
    },

    /* ── NOTIFY A SPECIFIC USER — cross-device, via Firestore ── */
    async sendNotifToUser(email, notif){
      try{
        if(!email) return;
        await addDoc(collection(db,'ff_notifs'), Object.assign({
          toEmail: email, ts: Date.now(), delivered:false
        }, notif));
      }catch(e){ console.warn('FSDB.sendNotifToUser error:',e); }
    },

    /* ── LISTEN FOR MY INCOMING NOTIFS ── */
    _notifsUnsub: null,
    listenMyNotifs(email, onNotif){
      try{
        if(this._notifsUnsub) this._notifsUnsub();
        if(!email) return;
        var q = query(collection(db,'ff_notifs'), where('toEmail','==',email));
        this._notifsUnsub = onSnapshot(q, function(snap){
          snap.docChanges().forEach(function(change){
            if(change.type==='added'){
              var data = change.doc.data();
              if(!data.delivered && typeof onNotif==='function') onNotif(data, change.doc.id);
            }
          });
        }, function(err){ console.warn('FSDB.listenMyNotifs error:',err); });
      }catch(e){ console.warn('FSDB.listenMyNotifs setup error:',e); }
    },
    stopListenMyNotifs(){
      if(this._notifsUnsub){ try{ this._notifsUnsub(); }catch(e){} this._notifsUnsub=null; }
    },
    async markNotifDelivered(docId){
      try{ await setDoc(doc(db,'ff_notifs',docId), {delivered:true}, {merge:true}); }catch(e){}
    },

    /* ── MIGRATE: localStorage → Firestore (ek baar) ── */
    async migrate(){
      const done = localStorage.getItem('ff_cloud_migrated');
      if(done) return;
      try{
        /* Migrate users */
        const localUsers = JSON.parse(localStorage.getItem('ff_users')||'{}');
        for(const email of Object.keys(localUsers)){
          await window.FSDB.saveUser(email, localUsers[email]);
        }
        /* Migrate cards */
        const localCards = JSON.parse(localStorage.getItem('ff_cardRegistry')||'[]');
        if(localCards.length) await window.FSDB.saveAllCards(localCards);
        /* Migrate bin */
        const localBin = JSON.parse(localStorage.getItem('ff_cardBin')||'[]');
        if(localBin.length) await window.FSDB.saveBin(localBin);

        localStorage.setItem('ff_cloud_migrated','1');
        console.log('✅ Firestore migration complete!');
      }catch(e){ console.warn('Migration error:',e); }
    }
  };


  /* Check redirect result on page load (mobile flow) */
  getRedirectResult(auth).then(function(result) {
    if(result && result.user) {
      /* handleGoogleUser is defined later in the file — wait a bit
         for the whole file to finish loading, otherwise a "not defined" error can occur */
      var _tries = 0;
      (function _waitAndCall(){
        if(typeof handleGoogleUser === 'function'){
          handleGoogleUser(result.user);
        } else if(_tries < 40){
          _tries++;
          setTimeout(_waitAndCall, 100);
        } else {
          alert('⚠️ Google login load nahi ho paya, page refresh karke dobara try karo.');
        }
      })();
    } else if(sessionStorage.getItem('_gLoginAttempted')){
      /* Humne login shuru kiya tha lekin result null aaya — matlab redirect fail hua */
      sessionStorage.removeItem('_gLoginAttempted');
      alert('⚠️ Google login se koi result nahi mila. Ho sakta hai browser third-party cookies/storage block kar raha ho. Chrome mein "Incognito" mode band karke, ya Settings > Site Settings > Cookies allow karke dobara try karo.');
    }
  }).catch(function(err){
    sessionStorage.removeItem('_gLoginAttempted');
    console.error('Redirect result error:', err);
    alert('⚠️ GOOGLE LOGIN ERROR:\n' + (err.code||'') + '\n' + (err.message||err));
  });

  window.firebaseGoogleLogin = async function() {
    try {
      if(typeof showToast === 'function') showToast('⏳ Google se connect ho raha hai...');
      /* Use popup only — on both mobile and desktop. The redirect method was returning
         a null result on mobile browsers (especially in-app browsers), due to
         cross-domain storage being blocked between GitHub Pages (custom domain) and Firebase authDomain. */
      const result = await signInWithPopup(auth, googleProvider);
      handleGoogleUser(result.user);
    } catch(err) {
      console.error('Firebase Google Login Error:', err);
      if(err.code === 'auth/popup-closed-by-user'){
        if(typeof showToast === 'function') showToast('ℹ️ Login cancel kar diya.');
      } else if(err.code === 'auth/popup-blocked' || err.code === 'auth/cancelled-popup-request'){
        /* If popup gets blocked, try redirect (fallback, better than nothing) */
        try{
          sessionStorage.setItem('_gLoginAttempted', '1');
          await signInWithRedirect(auth, googleProvider);
        }catch(err2){
          if(typeof showToast === 'function') showToast('❌ Popup bhi block ho gaya. Browser settings mein popups allow karo.');
        }
      } else {
        if(typeof showToast === 'function') showToast('❌ Google Login fail hua. Dobara try karo.');
      }
    }
  };

  window.firebaseSignOut = async function(){
    try{ await signOut(auth); }catch(e){}
  };

  /* ═══════════════ FIREBASE PHONE AUTH (real SMS OTP) ═══════════════ */
  window._recaptchaVerifier = null;
  window._confirmationResult = null;

  function _resetRecaptchaContainer(){
    var old = document.getElementById('recaptcha-container');
    if(old){
      var fresh = document.createElement('div');
      fresh.id = 'recaptcha-container';
      old.parentNode.replaceChild(fresh, old);
    }
  }

  function _getRecaptcha(){
    if(window._recaptchaVerifier) return window._recaptchaVerifier;
    _resetRecaptchaContainer();
    window._recaptchaVerifier = new RecaptchaVerifier(auth, 'recaptcha-container', { size: 'invisible' });
    return window._recaptchaVerifier;
  }

  /* fullPhone example: "+919876543210" (country code + 10 digit number, no spaces) */
  window.firebaseSendPhoneOTP = async function(fullPhone){
    try{
      var verifier = _getRecaptcha();
      var confirmation = await signInWithPhoneNumber(auth, fullPhone, verifier);
      window._confirmationResult = confirmation;
      return { ok:true };
    }catch(err){
      console.error('Firebase Phone OTP send error:', err);
      try{
        if(window._recaptchaVerifier){ window._recaptchaVerifier.clear(); }
      }catch(e){}
      window._recaptchaVerifier = null;
      _resetRecaptchaContainer();
      return { ok:false, code: err.code||'', message: err.message||String(err) };
    }
  };

  window.firebaseVerifyPhoneOTP = async function(code){
    try{
      if(!window._confirmationResult) return { ok:false, message:'Pehle OTP bhejna zaroori hai.' };
      var result = await window._confirmationResult.confirm(code);
      return { ok:true, user: result.user };
    }catch(err){
      console.error('Firebase Phone OTP verify error:', err);
      return { ok:false, code: err.code||'', message: err.message||String(err) };
    }
  };

  /* ═══════════════ FIREBASE FACEBOOK AUTH (Facebook SDK token → Firebase) ═══════════════ */
  window.firebaseFacebookSignIn = async function(accessToken){
    try{
      var credential = FacebookAuthProvider.credential(accessToken);
      var result = await signInWithCredential(auth, credential);
      return { ok:true, user: result.user };
    }catch(err){
      console.error('Firebase Facebook sign-in error:', err);
      return { ok:false, code: err.code||'', message: err.message||String(err) };
    }
  };