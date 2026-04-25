import React, { useState, useEffect } from 'react';
import { 
  StyleSheet, 
  Text, 
  View, 
  TouchableOpacity, 
  ScrollView, 
  ActivityIndicator, 
  Alert,
  Dimensions,
  Platform
} from 'react-native';
import * as DocumentPicker from 'expo-document-picker';
import { LineChart } from "react-native-gifted-charts";
import * as Linking from 'expo-linking';

// 1. FIREBASE AUTHENTICATION
import { initializeApp } from 'firebase/app';
import { getAuth, signInAnonymously, onAuthStateChanged } from 'firebase/auth';

// --- CONFIGURATION ---
const firebaseConfig = {
 apiKey: "AIzaSyCHd_zIkvqRS1_sbXtCppcaukLnhjt1PH8",
  authDomain: "mindful-server-487201-q7.firebaseapp.com",
  projectId: "mindful-server-487201-q7",
  storageBucket: "mindful-server-487201-q7.firebasestorage.app",
  messagingSenderId: "730158129642",
  appId: "1:730158129642:web:17e598ce5f0424d7a2d293",
  measurementId: "G-BCYGBVPGTR"
};

// Cloud Run Service API endpoint
const SERVICE_API_ENDPOINT = "https://serviceapi-730158129642.europe-west1.run.app/api"; 

// Initialize Firebase
const app = initializeApp(firebaseConfig);
const auth = getAuth(app);

export default function App() {
  const [user, setUser] = useState(null);
  const [petName, setPetName] = useState('Guppy');
  const [uploading, setUploading] = useState(false);
  const [loadingCharts, setLoadingCharts] = useState(true);
  const [chartData, setChartData] = useState([]);
  const [apiError, setApiError] = useState(null);
  const [processingStatus, setProcessingStatus] = useState(null);

  // NEW: track last uploaded recordId so we can poll status
  const [activeRecordId, setActiveRecordId] = useState(null);

  // --- DEEP LINKING LOGIC ---
  useEffect(() => {
    // Handle the case where the app was opened via a link from a closed state
    const getInitialUrl = async () => {
      const initialUrl = await Linking.getInitialURL();
      if (initialUrl) handleDeepLink(initialUrl);
    };

    // Handle the case where the app is already open in the background
    const subscription = Linking.addEventListener('url', (event) => {
      handleDeepLink(event.url);
    });

    getInitialUrl();
    return () => subscription.remove();
  }, []);

  //This will parse the petName Query parameter that is passed in with the deeplink from the platform team's page
  //Deep Link format on from the platform team leading to this page should look like: petwell://?petName=Fluffy
  const handleDeepLink = (url) => {
    const { queryParams } = Linking.parse(url);
    console.log("Deep Link Received:", url);
    console.log("Parsed Query Params:", queryParams);

    if (queryParams && queryParams.petName) {
      setPetName(queryParams.petName);
      // Trigger a refresh for the new pet's data
      fetchChartData(queryParams.petName);
    }
  };

  // --- AUTHENTICATION & INITIAL FETCH ---
  useEffect(() => {
    //We are using anonymous sign in that doesn't require actual login/password authentication for testing purposes at the moment
    //Remove the below initAuth section when platform team finishes implementing firebase email/password login in their app.js like so: 
    //import { signInWithEmailAndPassword } from "firebase/auth";
    //await signInWithEmailAndPassword(auth, email, password);
    const initAuth = async () => {
      try {
        await signInAnonymously(auth);
      } catch (err) {
        setApiError(`Authentication failed: ${err.message}`);
      }
    };
    initAuth();

      
    const unsubscribeAuth = onAuthStateChanged(auth, (currentUser) => {
      setUser(currentUser);
      if (currentUser) {
        fetchChartData(petName);
      }
    });

    return () => unsubscribeAuth();
  }, [petName]);

  //FETCH CHART DATA LOGIC
  //API call to query chart data from database in our backend that will be visualized on our graphs
  const fetchChartData = async (targetPetName) => {
    try {
      const token = await user.getIdToken(); // Firebase ID token being sent to backend that firebase UID is being extracted from
      const effectivePetName = targetPetName || petName;

      const response = await fetch(
        `${SERVICE_API_ENDPOINT}/get-pet-trends?petName=${encodeURIComponent(effectivePetName)}`,
        {
          method: "GET",
          headers: {
            Authorization: `Bearer ${token}`, 
          },
        }
      );

      if (!response.ok) throw new Error("Failed to fetch trends");

      const data = await response.json();
      const formattedData = (data.trends || []).map(item => ({
        value: item.value,
        label: item.label
      }));

      setChartData(formattedData);
      setLoadingCharts(false);
      return formattedData.length > 0; // only "done" if we actually got data
    } catch (error) {
      setLoadingCharts(false);
      return false;
    }
  };

  // NEW: poll backend for record processing status
  const fetchRecordStatus = async (recordId) => {
    const token = await user.getIdToken();
    const resp = await fetch(
      `${SERVICE_API_ENDPOINT}/medical-record-status?recordId=${encodeURIComponent(recordId)}`,
      {
        method: "GET",
        headers: { Authorization: `Bearer ${token}` },
      }
    );

    if (!resp.ok) {
      const txt = await resp.text();
      throw new Error(`Status check failed: ${txt}`);
    }
    return resp.json(); // { recordId, status, error, updatedAt }
  };

  // UPDATED: smarter polling that waits for COMPLETED
  const startPolling = (recordId) => {
    let attempts = 0;

    // Increase total wait time: 20 attempts * 3s = 60s
    const maxAttempts = 20;

    const interval = setInterval(async () => {
      attempts++;
      try {
        setProcessingStatus(`AI Analyzing... (${attempts}/${maxAttempts})`);

        const statusPayload = await fetchRecordStatus(recordId);
        const status = statusPayload.status;

        if (status === "FAILED") {
          clearInterval(interval);
          setUploading(false);
          setProcessingStatus(null);
          setApiError(statusPayload.error || "Processing failed");
          return;
        }

        if (status === "COMPLETED") {
          // now the lab_results rows should exist
          const hasData = await fetchChartData(petName);

          clearInterval(interval);
          setUploading(false);
          setProcessingStatus(null);

          if (hasData && Platform.OS !== 'web') {
            Alert.alert("Success", "Health trends have been updated!");
          }
          return;
        }

        // else: UPLOADING/PROCESSING -> keep polling
        if (attempts >= maxAttempts) {
          clearInterval(interval);
          setUploading(false);
          setProcessingStatus(null);
          setApiError("Timed out waiting for processing to finish.");
        }
      } catch (e) {
        // transient errors: keep trying but stop at maxAttempts
        if (attempts >= maxAttempts) {
          clearInterval(interval);
          setUploading(false);
          setProcessingStatus(null);
          setApiError(e.message);
        }
      }
    }, 3000);
  };

  // --- UPLOAD HANDLER ---
  //API call to create and request a signed upload URL in our backend. 
  //PUT signedURL uploads to the corresponding Google Cloud Storage Bucket Directory as defined by the URL
  const handlePickAndUpload = async () => {
    if (!user) {
      Alert.alert("Wait", "Please wait for authentication...");
      return;
    }
    setApiError(null);

    try {
      const result = await DocumentPicker.getDocumentAsync({
        type: ['application/pdf', 'image/*'],
        copyToCacheDirectory: true
      });

      if (result.canceled) return;

      const file = result.assets[0];
      setUploading(true);
      setProcessingStatus("Requesting Signed URL...");

      const token = await user.getIdToken();
      const targetUrl = `${SERVICE_API_ENDPOINT.replace(/\/$/, '')}/get-signed-url`;
      const contentType = file.mimeType || 'application/pdf';
      
      //API Call passes in petName, fileName, and file Content Type in JSON format to create signeduploadURL
      const urlResponse = await fetch(targetUrl, {
        method: 'POST',
        headers: {
          'Authorization': `Bearer ${token}`,
          'Content-Type': 'application/json'
        },
        body: JSON.stringify({
          petName: petName,
          fileName: file.name,
          contentType: contentType
        })
      });

      if (!urlResponse.ok) {
        const errText = await urlResponse.text();
        throw new Error(`Server Error: ${errText}`);
      }
      
      const { signedUrl, recordId } = await urlResponse.json();

      // NEW: keep recordId so polling targets the right record
      setActiveRecordId(recordId);

      setProcessingStatus("Uploading to Storage...");
      let body;
      try {
        const response = await fetch(file.uri);
        body = await response.blob();
      } catch (e) {
        body = file.file || file; 
      }

      //Uploads to the Google Cloud Storage Bucket
      const gcsResponse = await fetch(signedUrl, {
        method: 'PUT',
        headers: { 'Content-Type': contentType },
        body: body
      });

      if (!gcsResponse.ok) throw new Error("Cloud Storage upload failed.");

      // UPDATED: poll status for this recordId
      startPolling(recordId);

    } catch (error) {
      setApiError(error.message);
      setUploading(false);
      setProcessingStatus(null);
      if (Platform.OS !== 'web') {
        Alert.alert("Upload Error", error.message);
      }
    }
  };

  return (
    <ScrollView style={styles.container} contentContainerStyle={styles.content}>
      <View style={styles.header}>
        <View>
          <Text style={styles.title}>PetWell Hub</Text>
          <Text style={styles.subtitle}>Viewing context for: {petName}</Text>
        </View>
        <View style={styles.petBadge}><Text style={styles.petBadgeText}>Active</Text></View>
      </View>

      {apiError && (
        <View style={styles.errorBanner}>
          <Text style={styles.errorText}>{apiError}</Text>
          <TouchableOpacity onPress={() => setApiError(null)}>
            <Text style={styles.errorClose}>✕</Text>
          </TouchableOpacity>
        </View>
      )}
      
      <View style={styles.card}>
        <Text style={styles.cardTitle}>Medical Metrics</Text>
        {loadingCharts ? (
          <ActivityIndicator size="large" color="#6366f1" />
        ) : (
          <LineChart
            data={chartData}
            height={180}
            width={Dimensions.get('window').width - 80}
            spacing={90}
            initialSpacing={40}
            endSpacing={40}
            color="#6366f1"
            thickness={4}
            dataPointsColor="#4338ca"
            curved
            animateOnDataChange
            animationDuration={1000}
            noOfSections={3}
            yAxisThickness={0}
            xAxisThickness={1}
            xAxisColor="#cbd5e1"
            hideRules
          />
        )}
      </View>

      <View style={styles.uploadSection}>
        <Text style={styles.sectionLabel}>New Record</Text>
        <Text style={styles.description}>
          Select a report for {petName}. The chart will refresh automatically.
        </Text>

        <TouchableOpacity 
          style={[styles.button, uploading && styles.buttonDisabled]} 
          onPress={handlePickAndUpload}
          disabled={uploading}
        >
          {uploading ? (
            <View style={styles.loaderRow}>
              <ActivityIndicator color="#fff" />
              <Text style={styles.buttonText}> {processingStatus}</Text>
            </View>
          ) : (
            <Text style={styles.buttonText}>Pick & Upload Record</Text>
          )}
        </TouchableOpacity>
      </View>
    </ScrollView>
  );
}

const styles = StyleSheet.create({
  container: { flex: 1, backgroundColor: '#f1f5f9' },
  content: { padding: 20, paddingTop: 60 },
  header: { flexDirection: 'row', justifyContent: 'space-between', alignItems: 'center', marginBottom: 25 },
  title: { fontSize: 32, fontWeight: '900', color: '#0f172a' },
  subtitle: { fontSize: 13, color: '#64748b', fontWeight: '500' },
  petBadge: { backgroundColor: '#6366f1', paddingHorizontal: 12, paddingVertical: 6, borderRadius: 20 },
  petBadgeText: { color: '#fff', fontWeight: 'bold', fontSize: 12 },
  errorBanner: { backgroundColor: '#fee2e2', padding: 15, borderRadius: 12, flexDirection: 'row', marginBottom: 20, borderWidth: 1, borderColor: '#fecaca' },
  errorText: { color: '#b91c1c', fontSize: 13, fontWeight: '600', flex: 1 },
  errorClose: { color: '#b91c1c', marginLeft: 10, fontSize: 16, fontWeight: 'bold' },
  card: { backgroundColor: '#fff', borderRadius: 24, padding: 20, marginBottom: 20, shadowColor: '#6366f1', shadowOpacity: 0.1, shadowRadius: 20, elevation: 5 },
  cardTitle: { fontSize: 16, fontWeight: '700', color: '#64748b', marginBottom: 20 },
  uploadSection: { backgroundColor: '#fff', borderRadius: 24, padding: 24, borderStyle: 'dashed', borderWidth: 2, borderColor: '#cbd5e1' },
  sectionLabel: { fontSize: 18, fontWeight: '800', color: '#1e293b', marginBottom: 8 },
  description: { fontSize: 14, color: '#64748b', marginBottom: 24, lineHeight: 20 },
  button: { backgroundColor: '#6366f1', paddingVertical: 18, borderRadius: 16, alignItems: 'center', shadowColor: '#6366f1', shadowOpacity: 0.3, shadowRadius: 10, shadowOffset: { height: 5 } },
  buttonDisabled: { backgroundColor: '#cbd5e1' },
  buttonText: { color: '#fff', fontSize: 16, fontWeight: '800' },
  loaderRow: { flexDirection: 'row', alignItems: 'center' }
});
