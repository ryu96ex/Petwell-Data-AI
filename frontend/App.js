import React, { useState, useEffect, useMemo } from 'react';
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

// NEW: helper sleep function for backoff polling
const sleep = (ms) => new Promise((resolve) => setTimeout(resolve, ms));

export default function App() {
  const [user, setUser] = useState(null);
  const [petName, setPetName] = useState('Guppy');
  const [uploading, setUploading] = useState(false);
  const [loadingCharts, setLoadingCharts] = useState(true);
  const [chartData, setChartData] = useState([]);
  const [apiError, setApiError] = useState(null);
  const [processingStatus, setProcessingStatus] = useState(null);

   // NEW: simple "tooltip" state for hover/press point details
  const [selectedPoint, setSelectedPoint] = useState(null);

  // track last uploaded recordId (optional; mainly useful for debugging/UX)
  const [activeRecordId, setActiveRecordId] = useState(null);

  // --- DEEP LINKING LOGIC ---
  // Handle the case where the app was opened via a link from a closed state

  useEffect(() => {
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
      if (user) {
        fetchChartData(queryParams.petName);
      } else {
        // wait for onAuthStateChanged to set user, then it will fetch
        setLoadingCharts(true);
      }
    }
  
  };

  // --- AUTHENTICATION & INITIAL FETCH ---
  //We are using anonymous sign in that doesn't require actual login/password authentication for testing purposes at the moment
  //Remove the below initAuth section when platform team finishes implementing firebase email/password login in their app.js like so: 
  //import { signInWithEmailAndPassword } from "firebase/auth";
  //await signInWithEmailAndPassword(auth, email, password);
  useEffect(() => {
    
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

  // FETCH CHART DATA
  //API call to query chart data from database in our backend that will be visualized on our graphs
  // NOTE: returns true only if it got non-empty trends
  const fetchChartData = async (targetPetName) => {
  try {
    if (!user) throw new Error("User not authenticated");

    const token = await user.getIdToken();
    const effectivePetName = targetPetName || petName;

    const response = await fetch(
      `${SERVICE_API_ENDPOINT}/get-pet-trends?petName=${encodeURIComponent(effectivePetName)}`,
      {
        method: "GET",
        headers: { Authorization: `Bearer ${token}` },
      }
    );

    if (!response.ok) {
      const txt = await response.text();
      throw new Error(`Failed to fetch trends: ${txt}`);
    }

    const data = await response.json();
    console.log(data.trends)
    console.log("get-pet-trends raw:", data);

    const formattedData = (data.trends || []).map((item, idx) => {
      const v = Number(item.value);
      const label = item.label == null ? "" : String(item.label);

      if (!Number.isFinite(v)) {
        console.warn("Invalid chart value at", idx, item);
      }
      return { value: v, label };
    }).filter(p => Number.isFinite(p.value) && p.label.length > 0);

    console.log("chartData formatted:", formattedData);

    setChartData(formattedData);
    setLoadingCharts(false);

    return formattedData.length > 0;
  } catch (error) {
    console.error("fetchChartData failed:", error);
    setApiError(error.message);
    setLoadingCharts(false);
    return false;
  }
};

  // Poll backend for record processing status
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
    return resp.json();
  };

  // NEW: Larger-budget status polling with backoff (replaces startPolling interval)
  const waitForRecordCompletion = async (recordId, effectivePetName, options = {}) => {
    const totalBudgetMs = options.totalBudgetMs ?? 180_000; // 3 minutes default
    const backoffDelaysMs = options.backoffDelaysMs ?? [2000, 2000, 3000, 3000, 5000, 8000, 10000, 10000, 10000];

    const start = Date.now();
    let attempt = 0;

    while (Date.now() - start < totalBudgetMs) {
      attempt++;

      const elapsedSec = Math.floor((Date.now() - start) / 1000);
      setProcessingStatus(`AI Analyzing... ${elapsedSec}s`);

      try {
        const statusPayload = await fetchRecordStatus(recordId);
        const status = statusPayload.status;

        if (status === "FAILED") {
          throw new Error(statusPayload.error || "Processing failed");
        }

        if (status === "COMPLETED") {
          // once completed, fetch trends once
          const hasData = await fetchChartData(effectivePetName);
          return { completed: true, hasData };
        }
        // else: UPLOADING/PROCESSING -> continue
      } catch (e) {
        if (Date.now() - start > totalBudgetMs - 10_000) {
          throw e;
        }
      }

      const delay = backoffDelaysMs[Math.min(attempt - 1, backoffDelaysMs.length - 1)];
      await sleep(delay);
    }

    return { completed: false, hasData: false };
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

      // NEW: Larger-budget polling (instead of startPolling(recordId))
      setProcessingStatus("Upload complete. Waiting for analysis...");

      const resultPoll = await waitForRecordCompletion(recordId, petName, {
        totalBudgetMs: 180_000, // tweak to 120_000, 300_000, etc.
      });

      setUploading(false);
      setProcessingStatus(null);

      if (!resultPoll.completed) {
        setApiError("Timed out waiting for processing to finish. Please try again in a moment.");
        return;
      }

      if (!resultPoll.hasData) {
        setApiError("Processing completed, but no chartable trends were found for this metric.");
        return;
      }

      if (Platform.OS !== 'web') {
        Alert.alert("Success", "Health trends have been updated!");
      }

    } catch (error) {
      setApiError(error.message);
      setUploading(false);
      setProcessingStatus(null);
      if (Platform.OS !== 'web') {
        Alert.alert("Upload Error", error.message);
      }
    }
  };

  // Render a custom tooltip for gifted-charts (shown on press; on web it will feel like hover)
  const renderTooltip = (item, index) => {
    // item: your data item (value/label/...)
    // index: index in dataset
    return (
      <View style={styles.tooltip}>
        <Text style={styles.tooltipTitle}>{item?.label ?? `Point ${index + 1}`}</Text>
        <Text style={styles.tooltipValue}>{item?.value}</Text>
      </View>
    );
  };

  // Derive a padded max so the chart always has headroom
  const maxVal = useMemo(() => {
    if (!chartData || chartData.length === 0) return 100;
    const raw = Math.max(...chartData.map((d) => d.value));
    // Round up to a "nice" number with ~15% headroom
    return Math.ceil((raw * 1.15) / 10) * 10;
  }, [chartData]);


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

        {/* Optional: show a "selected point" readout (nice on mobile where hover isn't a thing) */}
        {selectedPoint && (
          <View style={styles.selectedPointRow}>
            <Text style={styles.selectedPointText}>
              Selected: {selectedPoint.label} • {selectedPoint.value}
            </Text>
          </View>
        )}

        {loadingCharts ? (
          <ActivityIndicator size="large" color="#6366f1" />
        ) : (
          
          <LineChart
            key={chartData.length}          // ← forces remount when point count changes
            //maxValue={maxVal} 
            data={chartData}
            showDataPointText
            dataPointTextColor="#111827"
            dataPointTextShiftY={-10}

            // Layout / spacing
            height={200}
            width={Dimensions.get('window').width - 80}
            spacing={90}
            initialSpacing={60}
            endSpacing={60}

            // Line + points
            color="#6366f1"
            thickness={3}
            dataPointsColor="#4338ca"
            curved={chartData.length > 2}
            dataPointsRadius={5}
            dataPointsWidth={2}
            dataPointsHeight={2}

            // Make it feel more "premium"
            areaChart
            startFillColor="rgba(99, 102, 241, 0.25)"
            endFillColor="rgba(99, 102, 241, 0.02)"
            startOpacity={0.9}
            endOpacity={0.05}

            // Axes / grid
            noOfSections={4}
            yAxisThickness={0}
            xAxisThickness={1}
            xAxisColor="#e2e8f0"
            rulesType="solid"
            rulesColor="#eef2ff"
            yAxisTextStyle={{ color: '#94a3b8', fontSize: 11 }}
            xAxisLabelTextStyle={{ color: '#94a3b8', fontSize: 11 }}

            // Interaction (gifted-charts shows tooltip on press; on web pointer events often behave like hover)
            pointerConfig={{
              pointerStripUptoDataPoint: true,
              pointerStripColor: 'rgba(99,102,241,0.25)',
              pointerStripWidth: 2,

              pointerColor: 'transparent',
              radius: 0,

              activatePointersOnLongPress: Platform.OS !== 'web',
              autoAdjustPointerLabelPosition: true,

              pointerLabelComponent: (items) => {
                // items is an array; for single line chart use items[0]
                const it = items?.[0];
                if (!it) return null;
                return (
                  <View style={{
                    minWidth: 160,          
                    paddingVertical: 10,    
                    paddingHorizontal: 12,
                    borderRadius: 12,
                    backgroundColor: '#0f172a',
                  }}>
                    <Text style={{ color: 'white', fontSize: 14, fontWeight: '700' }}>
                      {it.label}
                    </Text>
                    <Text style={{ color: 'white', fontSize: 18, fontWeight: '800', marginTop: 4 }}>
                      {it.value}
                    </Text>
                  </View>
                );
              },
            }}

            // Track which point was touched (better UX on mobile)
            onPress={(item, index) => setSelectedPoint(item)}

            // Existing behavior
            isAnimated
            hideRules={false}
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
  cardTitle: { fontSize: 16, fontWeight: '700', color: '#64748b', marginBottom: 12 },

  selectedPointRow: {
    backgroundColor: '#eef2ff',
    borderColor: '#c7d2fe',
    borderWidth: 1,
    paddingVertical: 8,
    paddingHorizontal: 10,
    borderRadius: 12,
    marginBottom: 12,
  },
  selectedPointText: { color: '#3730a3', fontSize: 12, fontWeight: '700' },

  tooltip: {
    backgroundColor: '#0f172a',
    paddingVertical: 8,
    paddingHorizontal: 10,
    borderRadius: 12,
    borderWidth: 1,
    borderColor: 'rgba(255,255,255,0.12)',
  },
  tooltipTitle: { color: '#cbd5e1', fontSize: 11, fontWeight: '700' },
  tooltipValue: { color: '#ffffff', fontSize: 14, fontWeight: '900', marginTop: 2 },

  uploadSection: { backgroundColor: '#fff', borderRadius: 24, padding: 24, borderStyle: 'dashed', borderWidth: 2, borderColor: '#cbd5e1' },
  sectionLabel: { fontSize: 18, fontWeight: '800', color: '#1e293b', marginBottom: 8 },
  description: { fontSize: 14, color: '#64748b', marginBottom: 24, lineHeight: 20 },
  button: { backgroundColor: '#6366f1', paddingVertical: 18, borderRadius: 16, alignItems: 'center', shadowColor: '#6366f1', shadowOpacity: 0.3, shadowRadius: 10, shadowOffset: { height: 5 } },
  buttonDisabled: { backgroundColor: '#cbd5e1' },
  buttonText: { color: '#fff', fontSize: 16, fontWeight: '800' },
  loaderRow: { flexDirection: 'row', alignItems: 'center' }
});
