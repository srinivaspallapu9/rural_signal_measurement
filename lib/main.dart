import 'package:flutter/material.dart';
import 'package:connectivity_plus/connectivity_plus.dart';
import 'package:geolocator/geolocator.dart';
import 'package:sqflite/sqflite.dart';
import 'package:path/path.dart' as path;
import 'dart:async';
import 'dart:io';
import 'dart:math';
import 'package:path_provider/path_provider.dart';

void main() {
  runApp(SignalMeasurementApp());
}

// Data Model
class SignalData {
  final int? id;
  final DateTime timestamp;
  final double latitude;
  final double longitude;
  final int rssi;
  final String networkType;
  final double? downloadSpeed;
  final double? uploadSpeed;
  final String? operatorName;
  final double? accuracy;
  final String? address;

  SignalData({
    this.id,
    required this.timestamp,
    required this.latitude,
    required this.longitude,
    required this.rssi,
    required this.networkType,
    this.downloadSpeed,
    this.uploadSpeed,
    this.operatorName,
    this.accuracy,
    this.address,
  });

  Map<String, dynamic> toMap() {
    return {
      'id': id,
      'timestamp': timestamp.millisecondsSinceEpoch,
      'latitude': latitude,
      'longitude': longitude,
      'rssi': rssi,
      'networkType': networkType,
      'downloadSpeed': downloadSpeed,
      'uploadSpeed': uploadSpeed,
      'operatorName': operatorName,
      'accuracy': accuracy,
      'address': address,
    };
  }

  static SignalData fromMap(Map<String, dynamic> map) {
    return SignalData(
      id: map['id'],
      timestamp: DateTime.fromMillisecondsSinceEpoch(map['timestamp']),
      latitude: map['latitude'],
      longitude: map['longitude'],
      rssi: map['rssi'],
      networkType: map['networkType'],
      downloadSpeed: map['downloadSpeed'],
      uploadSpeed: map['uploadSpeed'],
      operatorName: map['operatorName'],
      accuracy: map['accuracy'],
      address: map['address'],
    );
  }

  String toCsv() {
    return '${timestamp.toIso8601String()},$latitude,$longitude,$rssi,$networkType,${downloadSpeed ?? "N/A"},${uploadSpeed ?? "N/A"},${operatorName ?? "N/A"},${accuracy ?? "N/A"},${address ?? "N/A"}\n';
  }

  static String get csvHeader => 'Timestamp,Latitude,Longitude,RSSI (dBm),NetworkType,DownloadSpeed (Mbps),UploadSpeed (Mbps),Operator,Accuracy (m),Address\n';
}

// Database Service
class DatabaseService {
  static Database? _database;
  
  Future<Database> get database async {
    if (_database != null) return _database!;
    _database = await _initDatabase();
    return _database!;
  }

  Future<Database> _initDatabase() async {
    String dbPath = path.join(await getDatabasesPath(), 'signal_data.db');
    return await openDatabase(
      dbPath,
      version: 2,
      onCreate: (Database db, int version) async {
        await db.execute('''
          CREATE TABLE signal_data(
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            timestamp INTEGER NOT NULL,
            latitude REAL NOT NULL,
            longitude REAL NOT NULL,
            rssi INTEGER NOT NULL,
            networkType TEXT NOT NULL,
            downloadSpeed REAL,
            uploadSpeed REAL,
            operatorName TEXT,
            accuracy REAL,
            address TEXT
          )
        ''');
      },
      onUpgrade: (Database db, int oldVersion, int newVersion) async {
        if (oldVersion < 2) {
          await db.execute('ALTER TABLE signal_data ADD COLUMN address TEXT');
        }
      },
    );
  }

  Future<int> insertSignalData(SignalData data) async {
    final Database db = await database;
    return await db.insert('signal_data', data.toMap());
  }

  Future<List<SignalData>> getAllSignalData() async {
    final Database db = await database;
    final List<Map<String, dynamic>> maps = await db.query(
      'signal_data',
      orderBy: 'timestamp DESC',
    );
    return maps.map((map) => SignalData.fromMap(map)).toList();
  }

  Future<List<SignalData>> getSignalDataByOperator(String operator) async {
    final Database db = await database;
    final List<Map<String, dynamic>> maps = await db.query(
      'signal_data',
      where: 'operatorName = ?',
      whereArgs: [operator],
      orderBy: 'timestamp DESC',
    );
    return maps.map((map) => SignalData.fromMap(map)).toList();
  }

  Future<int> getRecordCount() async {
    final Database db = await database;
    return Sqflite.firstIntValue(
      await db.rawQuery('SELECT COUNT(*) FROM signal_data')
    ) ?? 0;
  }

  Future<int> deleteAllData() async {
    final Database db = await database;
    return await db.delete('signal_data');
  }

  Future<Map<String, dynamic>> getOperatorStats() async {
    final Database db = await database;
    final List<Map<String, dynamic>> maps = await db.rawQuery('''
      SELECT operatorName, 
             COUNT(*) as count,
             AVG(rssi) as avgRssi,
             AVG(downloadSpeed) as avgDownload,
             AVG(uploadSpeed) as avgUpload
      FROM signal_data 
      WHERE operatorName IS NOT NULL 
      GROUP BY operatorName
    ''');
    
    return {
      for (var map in maps)
        map['operatorName']: {
          'count': map['count'],
          'avgRssi': (map['avgRssi'] as double?)?.round() ?? 0,
          'avgDownload': (map['avgDownload'] as double?)?.toStringAsFixed(1) ?? '0.0',
          'avgUpload': (map['avgUpload'] as double?)?.toStringAsFixed(1) ?? '0.0',
        }
    };
  }
}

// Signal Service
class SignalService {
  final StreamController<SignalData> _signalController = StreamController<SignalData>.broadcast();
  final DatabaseService _databaseService = DatabaseService();
  
  Timer? _monitoringTimer;
  bool _isMonitoring = false;
  bool _isLogging = false;
  List<SignalData> _currentSessionData = [];

  Stream<SignalData> get signalStream => _signalController.stream;
  bool get isMonitoring => _isMonitoring;
  bool get isLogging => _isLogging;
  List<SignalData> get currentSessionData => _currentSessionData;

  Future<void> startMonitoring() async {
    if (_isMonitoring) return;
    
    _isMonitoring = true;
    const monitoringInterval = Duration(seconds: 5);
    
    // Get initial reading
    await _takeMeasurement();
    
    _monitoringTimer = Timer.periodic(monitoringInterval, (timer) async {
      await _takeMeasurement();
    });
  }

  void stopMonitoring() {
    _monitoringTimer?.cancel();
    _isMonitoring = false;
  }

  Future<void> _takeMeasurement() async {
    try {
      // Get location
      final position = await _getCurrentLocation();
      if (position == null) return;

      // Get network info
      final connectivityResult = await Connectivity().checkConnectivity();
      final networkType = _getNetworkType(connectivityResult);
      
      // Get signal strength (simulated)
      final rssi = _getSimulatedSignalStrength();
      
      final signalData = SignalData(
        timestamp: DateTime.now(),
        latitude: position.latitude,
        longitude: position.longitude,
        rssi: rssi,
        networkType: networkType,
        operatorName: await _getSimulatedOperator(),
        accuracy: position.accuracy,
        address: await _getSimulatedAddress(position.latitude, position.longitude),
      );

      _signalController.add(signalData);
      _currentSessionData.add(signalData);
      
      if (_isLogging) {
        await _databaseService.insertSignalData(signalData);
      }
    } catch (e) {
      print('Error taking measurement: $e');
    }
  }

  Future<Position?> _getCurrentLocation() async {
    try {
      bool serviceEnabled = await Geolocator.isLocationServiceEnabled();
      if (!serviceEnabled) return null;

      LocationPermission permission = await Geolocator.checkPermission();
      if (permission == LocationPermission.denied) {
        permission = await Geolocator.requestPermission();
        if (permission == LocationPermission.denied) {
          return null;
        }
      }

      if (permission == LocationPermission.deniedForever) {
        return null;
      }

      return await Geolocator.getCurrentPosition(
        desiredAccuracy: LocationAccuracy.best,
      );
    } catch (e) {
      print('Location error: $e');
      return null;
    }
  }

  String _getNetworkType(ConnectivityResult result) {
    switch (result) {
      case ConnectivityResult.mobile:
        return '4G'; // Simplified
      case ConnectivityResult.wifi:
        return 'WiFi';
      case ConnectivityResult.ethernet:
        return 'Ethernet';
      case ConnectivityResult.vpn:
        return 'VPN';
      case ConnectivityResult.bluetooth:
        return 'Bluetooth';
      default:
        return 'No Connection';
    }
  }

  int _getSimulatedSignalStrength() {
    // Simulate realistic signal strength variations
    final random = DateTime.now().millisecond % 100;
    if (random < 20) return -65 + (random % 15); // Excellent
    if (random < 50) return -80 + (random % 15); // Good
    if (random < 80) return -95 + (random % 15); // Fair
    return -110 + (random % 10); // Poor
  }

  Future<String> _getSimulatedOperator() async {
    final operators = ['Airtel', 'Jio', 'VI', 'BSNL', 'Unknown'];
    final random = DateTime.now().second % operators.length;
    return operators[random];
  }

  Future<String> _getSimulatedAddress(double lat, double lng) async {
    // Simulated address based on coordinates
    final areas = ['Rural Area A', 'Village Center', 'Farmland Zone', 'Market Area', 'Residential Zone'];
    final random = (lat * lng).abs().toInt() % areas.length;
    return '${areas[random]}, Lat: ${lat.toStringAsFixed(4)}, Lng: ${lng.toStringAsFixed(4)}';
  }

  void startLogging() {
    _isLogging = true;
  }

  void stopLogging() {
    _isLogging = false;
  }

  Future<List<SignalData>> getHistoricalData() async {
    return await _databaseService.getAllSignalData();
  }

  Future<List<SignalData>> getDataByOperator(String operator) async {
    return await _databaseService.getSignalDataByOperator(operator);
  }

  Future<String> exportToCsv() async {
    final data = await _databaseService.getAllSignalData();
    String csv = SignalData.csvHeader;
    for (var item in data) {
      csv += item.toCsv();
    }
    return csv;
  }

  Future<void> saveCsvToFile() async {
    try {
      final csvData = await exportToCsv();
      final directory = await getDownloadsDirectory();
      final file = File('${directory?.path}/signal_data_${DateTime.now().millisecondsSinceEpoch}.csv');
      await file.writeAsString(csvData);
    } catch (e) {
      print('Error saving CSV: $e');
    }
  }

  Future<Map<String, dynamic>> getStatistics() async {
    final data = await _databaseService.getAllSignalData();
    if (data.isEmpty) return {};
    
    final rssiValues = data.map((d) => d.rssi).toList();
    final downloadSpeeds = data.where((d) => d.downloadSpeed != null).map((d) => d.downloadSpeed!).toList();
    final uploadSpeeds = data.where((d) => d.uploadSpeed != null).map((d) => d.uploadSpeed!).toList();
    
    final avgRssi = rssiValues.reduce((a, b) => a + b) / rssiValues.length;
    final avgDownload = downloadSpeeds.isNotEmpty ? downloadSpeeds.reduce((a, b) => a + b) / downloadSpeeds.length : 0;
    final avgUpload = uploadSpeeds.isNotEmpty ? uploadSpeeds.reduce((a, b) => a + b) / uploadSpeeds.length : 0;

    return {
      'totalRecords': data.length,
      'avgSignalStrength': avgRssi.round(),
      'bestSignal': rssiValues.reduce((a, b) => a > b ? a : b),
      'worstSignal': rssiValues.reduce((a, b) => a < b ? a : b),
      'avgDownload': avgDownload.toStringAsFixed(1),
      'avgUpload': avgUpload.toStringAsFixed(1),
      'lastUpdate': data.first.timestamp,
      'operatorStats': await _databaseService.getOperatorStats(),
    };
  }

  Future<void> deleteAllData() async {
    await _databaseService.deleteAllData();
    _currentSessionData.clear();
  }

  void clearSessionData() {
    _currentSessionData.clear();
  }

  void dispose() {
    stopMonitoring();
    _signalController.close();
  }
}

// Speed Test Service
class SpeedTestService {
  Future<Map<String, double>> runSpeedTest() async {
    // Simulate speed test
    await Future.delayed(Duration(seconds: 3));
    
    final random = DateTime.now().millisecond % 100;
    return {
      'download': 5.0 + (random / 10), // 5-15 Mbps
      'upload': 1.0 + (random / 20),   // 1-6 Mbps
      'latency': 30.0 + (random % 70), // 30-100 ms
    };
  }
}

// Main App
class SignalMeasurementApp extends StatelessWidget {
  @override
  Widget build(BuildContext context) {
    return MaterialApp(
      title: 'Rural Signal Measurement',
      theme: ThemeData(
        primarySwatch: Colors.blue,
        useMaterial3: true,
      ),
      home: MainNavigationScreen(),
      debugShowCheckedModeBanner: false,
    );
  }
}

// Main Navigation Screen with Bottom Navigation
class MainNavigationScreen extends StatefulWidget {
  @override
  _MainNavigationScreenState createState() => _MainNavigationScreenState();
}

class _MainNavigationScreenState extends State<MainNavigationScreen> {
  int _currentIndex = 0;
  final SignalService _signalService = SignalService();

  late final List<Widget> _pages;

  @override
  void initState() {
    super.initState();
    _pages = [
      HomeScreen(signalService: _signalService),
      DataAnalysisScreen(signalService: _signalService),
      OperatorComparisonScreen(signalService: _signalService),
      SettingsScreen(signalService: _signalService),
    ];
  }

  @override
  void dispose() {
    _signalService.dispose();
    super.dispose();
  }

  @override
  Widget build(BuildContext context) {
    return Scaffold(
      body: _pages[_currentIndex],
      bottomNavigationBar: BottomNavigationBar(
        currentIndex: _currentIndex,
        onTap: (index) {
          setState(() {
            _currentIndex = index;
          });
        },
        type: BottomNavigationBarType.fixed,
        items: [
          BottomNavigationBarItem(
            icon: Icon(Icons.home),
            label: 'Home',
          ),
          BottomNavigationBarItem(
            icon: Icon(Icons.analytics),
            label: 'Analysis',
          ),
          BottomNavigationBarItem(
            icon: Icon(Icons.compare),
            label: 'Compare',
          ),
          BottomNavigationBarItem(
            icon: Icon(Icons.settings),
            label: 'Settings',
          ),
        ],
      ),
    );
  }
}

// Home Screen
class HomeScreen extends StatefulWidget {
  final SignalService signalService;

  HomeScreen({required this.signalService});

  @override
  _HomeScreenState createState() => _HomeScreenState();
}

class _HomeScreenState extends State<HomeScreen> {
  final SpeedTestService _speedTestService = SpeedTestService();
  
  String _downloadSpeed = '--';
  String _uploadSpeed = '--';
  Map<String, dynamic> _stats = {};
  bool _isTestingSpeed = false;

  @override
  void initState() {
    super.initState();
    _loadStatistics();
  }

  Future<void> _loadStatistics() async {
    final stats = await widget.signalService.getStatistics();
    setState(() {
      _stats = stats;
    });
  }

  void _showMessage(String message) {
    ScaffoldMessenger.of(context).showSnackBar(
      SnackBar(content: Text(message)),
    );
  }

  @override
  Widget build(BuildContext context) {
    return Scaffold(
      appBar: AppBar(
        title: Text('Signal Measurement'),
        backgroundColor: Colors.blue[700],
        actions: [
          PopupMenuButton<String>(
            onSelected: (value) {
              if (value == 'export') _exportData();
              if (value == 'clear') _clearData();
              if (value == 'stats') _showStatistics();
            },
            itemBuilder: (BuildContext buildContext) => [
              PopupMenuItem(value: 'export', child: Text('Export Data')),
              PopupMenuItem(value: 'clear', child: Text('Clear Data')),
              PopupMenuItem(value: 'stats', child: Text('View Statistics')),
            ],
          ),
        ],
      ),
      body: StreamBuilder<SignalData>(
        stream: widget.signalService.signalStream,
        builder: (BuildContext streamContext, AsyncSnapshot<SignalData> snapshot) {
          final signalData = snapshot.data;
          
          return Padding(
            padding: EdgeInsets.all(16.0),
            child: ListView(
              children: [
                // Status Card
                _buildStatusCard(signalData),
                SizedBox(height: 16),
                
                // Signal Gauge Card
                _buildSignalGaugeCard(signalData),
                SizedBox(height: 16),
                
                // Network Info Card
                _buildNetworkCard(signalData),
                SizedBox(height: 16),
                
                // Speed Test Card
                _buildSpeedTestCard(),
                SizedBox(height: 16),
                
                // Quick Stats Card
                _buildQuickStatsCard(),
              ],
            ),
          );
        },
      ),
    );
  }

  Widget _buildStatusCard(SignalData? data) {
    return Card(
      elevation: 4,
      child: Padding(
        padding: EdgeInsets.all(16.0),
        child: Row(
          children: [
            Icon(
              widget.signalService.isMonitoring ? Icons.play_arrow : Icons.pause,
              color: widget.signalService.isMonitoring ? Colors.green : Colors.grey,
              size: 32,
            ),
            SizedBox(width: 16),
            Expanded(
              child: Column(
                crossAxisAlignment: CrossAxisAlignment.start,
                children: [
                  Text(
                    widget.signalService.isMonitoring ? 'Monitoring Active' : 'Monitoring Paused',
                    style: TextStyle(fontSize: 18, fontWeight: FontWeight.bold),
                  ),
                  SizedBox(height: 4),
                  Text(
                    widget.signalService.isLogging ? '📊 Logging Data' : '⏸️ Logging Paused',
                    style: TextStyle(color: Colors.grey),
                  ),
                ],
              ),
            ),
            Column(
              children: [
                Chip(
                  label: Text('${widget.signalService.currentSessionData.length} session'),
                  backgroundColor: Colors.blue[50],
                ),
                SizedBox(height: 4),
                Chip(
                  label: Text('${_stats['totalRecords'] ?? 0} total'),
                  backgroundColor: Colors.green[50],
                ),
              ],
            ),
          ],
        ),
      ),
    );
  }

  Widget _buildSignalGaugeCard(SignalData? data) {
    final rssi = data?.rssi ?? -100;
    final signalLevel = _getSignalLevel(rssi);
    final signalColor = _getSignalColor(signalLevel);
    
    return Card(
      elevation: 4,
      child: Padding(
        padding: EdgeInsets.all(16.0),
        child: Column(
          children: [
            Text(
              'Signal Strength Gauge',
              style: TextStyle(fontSize: 18, fontWeight: FontWeight.bold),
            ),
            SizedBox(height: 16),
            Container(
              height: 200,
              child: Stack(
                alignment: Alignment.center,
                children: [
                  // Background Circle
                  Container(
                    width: 180,
                    height: 180,
                    decoration: BoxDecoration(
                      shape: BoxShape.circle,
                      border: Border.all(color: Colors.grey[300]!, width: 10),
                    ),
                  ),
                  
                  // Signal Level Arc
                  CustomPaint(
                    size: Size(180, 180),
                    painter: _SignalGaugePainter(rssi: rssi, signalColor: signalColor),
                  ),
                  
                  // Center Text
                  Column(
                    mainAxisAlignment: MainAxisAlignment.center,
                    children: [
                      Text(
                        '$rssi dBm',
                        style: TextStyle(fontSize: 24, fontWeight: FontWeight.bold),
                      ),
                      Text(
                        signalLevel,
                        style: TextStyle(fontSize: 16, color: signalColor, fontWeight: FontWeight.bold),
                      ),
                    ],
                  ),
                ],
              ),
            ),
            SizedBox(height: 16),
            _buildSignalLegend(),
          ],
        ),
      ),
    );
  }

  Widget _buildSignalLegend() {
    return Row(
      mainAxisAlignment: MainAxisAlignment.spaceAround,
      children: [
        _buildLegendItem('Excellent', Colors.green),
        _buildLegendItem('Good', Colors.lightGreen),
        _buildLegendItem('Fair', Colors.orange),
        _buildLegendItem('Poor', Colors.red),
      ],
    );
  }

  Widget _buildLegendItem(String text, Color color) {
    return Row(
      children: [
        Container(
          width: 12,
          height: 12,
          decoration: BoxDecoration(
            color: color,
            shape: BoxShape.circle,
          ),
        ),
        SizedBox(width: 4),
        Text(text, style: TextStyle(fontSize: 10)),
      ],
    );
  }

  Widget _buildNetworkCard(SignalData? data) {
    return Card(
      elevation: 4,
      child: Padding(
        padding: EdgeInsets.all(16.0),
        child: Column(
          crossAxisAlignment: CrossAxisAlignment.start,
          children: [
            Text(
              'Network Information',
              style: TextStyle(fontSize: 18, fontWeight: FontWeight.bold),
            ),
            SizedBox(height: 12),
            _buildInfoRow('Network Type', data?.networkType ?? 'Unknown', Icons.network_cell),
            _buildInfoRow('Operator', data?.operatorName ?? 'Unknown', Icons.business),
            _buildInfoRow('Location', 
              data != null ? '${data.latitude.toStringAsFixed(4)}, ${data.longitude.toStringAsFixed(4)}' : 'Unknown', 
              Icons.location_on
            ),
            _buildInfoRow('Accuracy', 
              data?.accuracy != null ? '±${data!.accuracy!.toStringAsFixed(1)}m' : 'Unknown', 
              Icons.gps_fixed
            ),
            if (data?.address != null)
              _buildInfoRow('Area', data!.address!, Icons.place),
          ],
        ),
      ),
    );
  }

  Widget _buildInfoRow(String label, String value, IconData icon) {
    return Padding(
      padding: EdgeInsets.symmetric(vertical: 6.0),
      child: Row(
        children: [
          Icon(icon, size: 20, color: Colors.blue),
          SizedBox(width: 12),
          Expanded(
            child: Text(label, style: TextStyle(fontWeight: FontWeight.w500)),
          ),
          Expanded(
            flex: 2,
            child: Text(value, 
              style: TextStyle(color: Colors.grey[700]),
              textAlign: TextAlign.right,
            ),
          ),
        ],
      ),
    );
  }

  Widget _buildSpeedTestCard() {
    return Card(
      elevation: 4,
      child: Padding(
        padding: EdgeInsets.all(16.0),
        child: Column(
          crossAxisAlignment: CrossAxisAlignment.start,
          children: [
            Text(
              'Speed Test',
              style: TextStyle(fontSize: 18, fontWeight: FontWeight.bold),
            ),
            SizedBox(height: 12),
            Row(
              mainAxisAlignment: MainAxisAlignment.spaceAround,
              children: [
                _buildSpeedIndicator('Download', _downloadSpeed, Icons.download),
                _buildSpeedIndicator('Upload', _uploadSpeed, Icons.upload),
              ],
            ),
            SizedBox(height: 12),
            Center(
              child: ElevatedButton.icon(
                onPressed: _isTestingSpeed ? null : _runSpeedTest,
                icon: _isTestingSpeed 
                    ? SizedBox(height: 16, width: 16, child: CircularProgressIndicator(strokeWidth: 2))
                    : Icon(Icons.speed),
                label: Text(_isTestingSpeed ? 'Testing...' : 'Run Speed Test'),
                style: ElevatedButton.styleFrom(
                  backgroundColor: Colors.green,
                  foregroundColor: Colors.white,
                ),
              ),
            ),
          ],
        ),
      ),
    );
  }

  Widget _buildSpeedIndicator(String label, String value, IconData icon) {
    return Column(
      children: [
        Icon(icon, size: 32, color: Colors.blue),
        SizedBox(height: 8),
        Text(label, style: TextStyle(color: Colors.grey)),
        SizedBox(height: 4),
        Text(value, style: TextStyle(fontSize: 18, fontWeight: FontWeight.bold)),
        Text('Mbps', style: TextStyle(color: Colors.grey, fontSize: 12)),
      ],
    );
  }

  Widget _buildQuickStatsCard() {
    return Card(
      elevation: 4,
      child: Padding(
        padding: EdgeInsets.all(16.0),
        child: Column(
          crossAxisAlignment: CrossAxisAlignment.start,
          children: [
            Text(
              'Quick Statistics',
              style: TextStyle(fontSize: 18, fontWeight: FontWeight.bold),
            ),
            SizedBox(height: 12),
            if (_stats.isEmpty)
              Text('No data collected yet', style: TextStyle(color: Colors.grey)),
            if (_stats.isNotEmpty) ...[
              _buildStatRow('Total Records', _stats['totalRecords'].toString()),
              _buildStatRow('Average Signal', '${_stats['avgSignalStrength']} dBm'),
              _buildStatRow('Best Signal', '${_stats['bestSignal']} dBm'),
              _buildStatRow('Worst Signal', '${_stats['worstSignal']} dBm'),
              if (_stats['avgDownload'] != null)
                _buildStatRow('Avg Download', '${_stats['avgDownload']} Mbps'),
              if (_stats['avgUpload'] != null)
                _buildStatRow('Avg Upload', '${_stats['avgUpload']} Mbps'),
            ],
            SizedBox(height: 12),
            _buildControlButtons(),
          ],
        ),
      ),
    );
  }

  Widget _buildStatRow(String label, String value) {
    return Padding(
      padding: EdgeInsets.symmetric(vertical: 4.0),
      child: Row(
        mainAxisAlignment: MainAxisAlignment.spaceBetween,
        children: [
          Text(label, style: TextStyle(color: Colors.grey)),
          Text(value, style: TextStyle(fontWeight: FontWeight.bold)),
        ],
      ),
    );
  }

  Widget _buildControlButtons() {
    return Column(
      children: [
        Row(
          children: [
            Expanded(
              child: ElevatedButton(
                onPressed: widget.signalService.isMonitoring ? _stopMonitoring : _startMonitoring,
                style: ElevatedButton.styleFrom(
                  backgroundColor: widget.signalService.isMonitoring ? Colors.red : Colors.green,
                  padding: EdgeInsets.symmetric(vertical: 15),
                ),
                child: Text(
                  widget.signalService.isMonitoring ? 'STOP MONITORING' : 'START MONITORING',
                  style: TextStyle(color: Colors.white, fontSize: 16),
                ),
              ),
            ),
          ],
        ),
        SizedBox(height: 10),
        Row(
          children: [
            Expanded(
              child: OutlinedButton(
                onPressed: widget.signalService.isLogging ? _stopLogging : _startLogging,
                child: Text(widget.signalService.isLogging ? 'STOP LOGGING' : 'START LOGGING'),
              ),
            ),
            SizedBox(width: 10),
            Expanded(
              child: OutlinedButton(
                onPressed: _exportData,
                child: Text('EXPORT DATA'),
              ),
            ),
          ],
        ),
      ],
    );
  }

  // Helper methods
  String _getSignalLevel(int rssi) {
    if (rssi >= -70) return 'Excellent';
    if (rssi >= -85) return 'Good';
    if (rssi >= -100) return 'Fair';
    return 'Poor';
  }

  Color _getSignalColor(String level) {
    switch (level) {
      case 'Excellent': return Colors.green;
      case 'Good': return Colors.lightGreen;
      case 'Fair': return Colors.orange;
      case 'Poor': return Colors.red;
      default: return Colors.grey;
    }
  }

  // Action methods
  void _startMonitoring() {
    widget.signalService.startMonitoring();
    setState(() {});
  }

  void _stopMonitoring() {
    widget.signalService.stopMonitoring();
    setState(() {});
  }

  void _startLogging() {
    widget.signalService.startLogging();
    setState(() {});
    _showMessage('Started logging data to database');
  }

  void _stopLogging() {
    widget.signalService.stopLogging();
    setState(() {});
    _showMessage('Stopped logging data');
  }

  Future<void> _runSpeedTest() async {
    setState(() {
      _isTestingSpeed = true;
      _downloadSpeed = '...';
      _uploadSpeed = '...';
    });

    try {
      final results = await _speedTestService.runSpeedTest();
      setState(() {
        _downloadSpeed = results['download']!.toStringAsFixed(1);
        _uploadSpeed = results['upload']!.toStringAsFixed(1);
      });
    } catch (e) {
      setState(() {
        _downloadSpeed = 'Error';
        _uploadSpeed = 'Error';
      });
    } finally {
      setState(() {
        _isTestingSpeed = false;
      });
    }
  }

  Future<void> _exportData() async {
    try {
      await widget.signalService.saveCsvToFile();
      _showMessage('Data exported successfully!');
    } catch (e) {
      _showMessage('Error exporting data: $e');
    }
  }

  Future<void> _clearData() async {
    final confirmed = await showDialog<bool>(
      context: context,
      builder: (BuildContext dialogContext) => AlertDialog(
        title: Text('Clear All Data?'),
        content: Text('This will permanently delete all collected signal data.'),
        actions: [
          TextButton(
            onPressed: () => Navigator.pop(dialogContext, false),
            child: Text('Cancel'),
          ),
          TextButton(
            onPressed: () => Navigator.pop(dialogContext, true),
            child: Text('Clear', style: TextStyle(color: Colors.red)),
          ),
        ],
      ),
    );

    if (confirmed == true) {
      await widget.signalService.deleteAllData();
      widget.signalService.clearSessionData();
      await _loadStatistics();
      setState(() {});
      _showMessage('All data cleared');
    }
  }

  void _showStatistics() {
    showDialog(
      context: context,
      builder: (BuildContext dialogContext) => AlertDialog(
        title: Text('Database Statistics'),
        content: FutureBuilder<List<SignalData>>(
          future: widget.signalService.getHistoricalData(),
          builder: (BuildContext futureContext, AsyncSnapshot<List<SignalData>> snapshot) {
            if (snapshot.connectionState == ConnectionState.waiting) {
              return Center(child: CircularProgressIndicator());
            }
            
            final data = snapshot.data ?? [];
            if (data.isEmpty) {
              return Text('No data available');
            }
            
            return Column(
              mainAxisSize: MainAxisSize.min,
              crossAxisAlignment: CrossAxisAlignment.start,
              children: [
                Text('Total Records: ${data.length}'),
                SizedBox(height: 8),
                Text('Date Range:'),
                Text('  From: ${data.last.timestamp.toString().substring(0, 16)}'),
                Text('  To: ${data.first.timestamp.toString().substring(0, 16)}'),
                SizedBox(height: 8),
                Text('Coverage Area:'),
                Text('  Lat: ${data.map((d) => d.latitude).reduce((a, b) => a < b ? a : b).toStringAsFixed(4)} to ${data.map((d) => d.latitude).reduce((a, b) => a > b ? a : b).toStringAsFixed(4)}'),
                Text('  Lng: ${data.map((d) => d.longitude).reduce((a, b) => a < b ? a : b).toStringAsFixed(4)} to ${data.map((d) => d.longitude).reduce((a, b) => a > b ? a : b).toStringAsFixed(4)}'),
              ],
            );
          },
        ),
        actions: [
          TextButton(
            onPressed: () => Navigator.pop(dialogContext),
            child: Text('Close'),
          ),
        ],
      ),
    );
  }
}

// Custom Painter for Signal Gauge
class _SignalGaugePainter extends CustomPainter {
  final int rssi;
  final Color signalColor;

  _SignalGaugePainter({required this.rssi, required this.signalColor});

  @override
  void paint(Canvas canvas, Size size) {
    final center = Offset(size.width / 2, size.height / 2);
    final radius = size.width / 2 - 10;
    
    // Calculate angle based on RSSI (-120 to -50)
    final double progress = (rssi + 120) / 70; // Convert to 0-1 range
    final double sweepAngle = 2 * 3.14159 * progress;
    
    final paint = Paint()
      ..color = signalColor
      ..style = PaintingStyle.stroke
      ..strokeWidth = 10
      ..strokeCap = StrokeCap.round;
    
    canvas.drawArc(
      Rect.fromCircle(center: center, radius: radius),
      -3.14159 / 2, // Start from top
      sweepAngle,
      false,
      paint,
    );
  }

  @override
  bool shouldRepaint(covariant CustomPainter oldDelegate) => true;
}

// Enhanced Data Analysis Screen
class DataAnalysisScreen extends StatefulWidget {
  final SignalService signalService;

  DataAnalysisScreen({required this.signalService});

  @override
  _DataAnalysisScreenState createState() => _DataAnalysisScreenState();
}

class _DataAnalysisScreenState extends State<DataAnalysisScreen> with SingleTickerProviderStateMixin {
  Map<String, dynamic> _stats = {};
  List<SignalData> _allData = [];
  late TabController _tabController;
  String _timeFilter = 'All';
  List<String> _timeFilters = ['All', 'Today', 'Week', 'Month'];
  bool _isLoading = true;

  @override
  void initState() {
    super.initState();
    _tabController = TabController(length: 4, vsync: this);
    _loadData();
  }

  @override
  void dispose() {
    _tabController.dispose();
    super.dispose();
  }

  Future<void> _loadData() async {
    try {
      final stats = await widget.signalService.getStatistics();
      final allData = await widget.signalService.getHistoricalData();
      
      setState(() {
        _stats = stats;
        _allData = allData;
        _isLoading = false;
      });
    } catch (e) {
      print("Error loading data: $e");
      setState(() {
        _isLoading = false;
      });
    }
  }

  List<SignalData> _getFilteredData() {
    final now = DateTime.now();
    switch (_timeFilter) {
      case 'Today':
        return _allData.where((data) => 
          data.timestamp.day == now.day && 
          data.timestamp.month == now.month && 
          data.timestamp.year == now.year
        ).toList();
      case 'Week':
        final weekAgo = now.subtract(Duration(days: 7));
        return _allData.where((data) => data.timestamp.isAfter(weekAgo)).toList();
      case 'Month':
        final monthAgo = now.subtract(Duration(days: 30));
        return _allData.where((data) => data.timestamp.isAfter(monthAgo)).toList();
      default:
        return _allData;
    }
  }

  @override
  Widget build(BuildContext context) {
    final filteredData = _getFilteredData();
    
    return Scaffold(
      appBar: AppBar(
        title: Text('Data Analysis'),
        backgroundColor: Colors.purple[700],
        bottom: _allData.isNotEmpty ? TabBar(
          controller: _tabController,
          isScrollable: true,
          tabs: [
            Tab(text: 'Overview'),
            Tab(text: 'Trends'),
            Tab(text: 'Distribution'),
            Tab(text: 'Quality'),
          ],
        ) : null,
      ),
      body: _isLoading 
          ? _buildLoadingState()
          : _allData.isEmpty 
            ? _buildEmptyState() 
            : _buildAnalysisContent(filteredData),
    );
  }

  Widget _buildLoadingState() {
    return Center(
      child: Column(
        mainAxisAlignment: MainAxisAlignment.center,
        children: [
          CircularProgressIndicator(),
          SizedBox(height: 16),
          Text('Loading data...'),
        ],
      ),
    );
  }

  Widget _buildEmptyState() {
    return Center(
      child: Column(
        mainAxisAlignment: MainAxisAlignment.center,
        children: [
          Icon(Icons.analytics, size: 64, color: Colors.grey),
          SizedBox(height: 16),
          Text(
            'No Data Available',
            style: TextStyle(fontSize: 18, color: Colors.grey),
          ),
          SizedBox(height: 8),
          Text(
            'Start monitoring on the Home screen to collect data',
            style: TextStyle(color: Colors.grey),
            textAlign: TextAlign.center,
          ),
        ],
      ),
    );
  }

  Widget _buildAnalysisContent(List<SignalData> data) {
    return Column(
      children: [
        // Time Filter
        Padding(
          padding: EdgeInsets.all(16),
          child: Row(
            children: [
              Text('Time Filter: ', style: TextStyle(fontWeight: FontWeight.bold)),
              SizedBox(width: 10),
              ..._timeFilters.map((filter) => Padding(
                padding: EdgeInsets.only(right: 8),
                child: ChoiceChip(
                  label: Text(filter),
                  selected: _timeFilter == filter,
                  onSelected: (selected) {
                    setState(() {
                      _timeFilter = filter;
                    });
                  },
                ),
              )).toList(),
            ],
          ),
        ),
        
        // Summary Cards
        _buildSummaryCards(data),
        
        // Tab Content
        Expanded(
          child: TabBarView(
            controller: _tabController,
            children: [
              _buildOverviewTab(data),
              _buildTrendsTab(data),
              _buildDistributionTab(data),
              _buildQualityTab(data),
            ],
          ),
        ),
      ],
    );
  }

  Widget _buildSummaryCards(List<SignalData> data) {
    if (data.isEmpty) return SizedBox();
    
    final rssiValues = data.map((d) => d.rssi).toList();
    final avgRssi = rssiValues.reduce((a, b) => a + b) / rssiValues.length;
    final bestRssi = rssiValues.reduce((a, b) => a > b ? a : b);
    final worstRssi = rssiValues.reduce((a, b) => a < b ? a : b);
    
    // Calculate coverage quality
    final excellentCount = data.where((d) => d.rssi >= -70).length;
    final poorCount = data.where((d) => d.rssi <= -100).length;
    final coverageQuality = data.isEmpty ? 0 : (excellentCount / data.length * 100);

    return Padding(
      padding: EdgeInsets.symmetric(horizontal: 16),
      child: Wrap(
        spacing: 10,
        runSpacing: 10,
        children: [
          _buildSummaryCard('Records', data.length.toString(), Icons.data_array, Colors.blue),
          _buildSummaryCard('Avg Signal', '${avgRssi.round()} dBm', Icons.signal_cellular_alt, Colors.green),
          _buildSummaryCard('Best Signal', '$bestRssi dBm', Icons.trending_up, Colors.lightGreen),
          _buildSummaryCard('Worst Signal', '$worstRssi dBm', Icons.trending_down, Colors.orange),
          _buildSummaryCard('Quality', '${coverageQuality.toStringAsFixed(1)}%', Icons.assessment, Colors.purple),
          _buildSummaryCard('Poor Areas', '$poorCount', Icons.warning, Colors.red),
        ],
      ),
    );
  }

  Widget _buildSummaryCard(String title, String value, IconData icon, Color color) {
    return Card(
      elevation: 3,
      child: Container(
        width: 110,
        padding: EdgeInsets.all(12),
        child: Column(
          children: [
            Icon(icon, size: 24, color: color),
            SizedBox(height: 8),
            Text(value, style: TextStyle(fontSize: 14, fontWeight: FontWeight.bold)),
            SizedBox(height: 4),
            Text(title, style: TextStyle(fontSize: 10, color: Colors.grey), textAlign: TextAlign.center),
          ],
        ),
      ),
    );
  }

  Widget _buildOverviewTab(List<SignalData> data) {
    return ListView(
      padding: EdgeInsets.all(16),
      children: [
        // Signal Strength Card
        Card(
          elevation: 4,
          child: Padding(
            padding: EdgeInsets.all(16),
            child: Column(
              crossAxisAlignment: CrossAxisAlignment.start,
              children: [
                Text('Signal Strength Analysis', style: TextStyle(fontSize: 18, fontWeight: FontWeight.bold)),
                SizedBox(height: 16),
                _buildSignalStrengthAnalysis(data),
              ],
            ),
          ),
        ),
        SizedBox(height: 16),
        
        // Network Type Analysis
        Card(
          elevation: 4,
          child: Padding(
            padding: EdgeInsets.all(16),
            child: Column(
              crossAxisAlignment: CrossAxisAlignment.start,
              children: [
                Text('Network Type Distribution', style: TextStyle(fontSize: 18, fontWeight: FontWeight.bold)),
                SizedBox(height: 16),
                _buildNetworkTypeAnalysis(data),
              ],
            ),
          ),
        ),
        SizedBox(height: 16),
        
        // Location Coverage
        Card(
          elevation: 4,
          child: Padding(
            padding: EdgeInsets.all(16),
            child: Column(
              crossAxisAlignment: CrossAxisAlignment.start,
              children: [
                Text('Location Coverage', style: TextStyle(fontSize: 18, fontWeight: FontWeight.bold)),
                SizedBox(height: 16),
                _buildLocationCoverage(data),
              ],
            ),
          ),
        ),
      ],
    );
  }

  Widget _buildTrendsTab(List<SignalData> data) {
    return ListView(
      padding: EdgeInsets.all(16),
      children: [
        Card(
          elevation: 4,
          child: Padding(
            padding: EdgeInsets.all(16),
            child: Column(
              crossAxisAlignment: CrossAxisAlignment.start,
              children: [
                Text('Hourly Signal Trends', style: TextStyle(fontSize: 18, fontWeight: FontWeight.bold)),
                SizedBox(height: 16),
                Container(
                  height: 200,
                  child: Center(
                    child: Text('Hourly trends chart would be shown here\n\nData points: ${data.length}', 
                      textAlign: TextAlign.center,
                    ),
                  ),
                ),
              ],
            ),
          ),
        ),
        SizedBox(height: 16),
        
        Card(
          elevation: 4,
          child: Padding(
            padding: EdgeInsets.all(16),
            child: Column(
              crossAxisAlignment: CrossAxisAlignment.start,
              children: [
                Text('Time-based Performance', style: TextStyle(fontSize: 18, fontWeight: FontWeight.bold)),
                SizedBox(height: 16),
                _buildTimeAnalysis(data),
              ],
            ),
          ),
        ),
      ],
    );
  }

  Widget _buildDistributionTab(List<SignalData> data) {
    return ListView(
      padding: EdgeInsets.all(16),
      children: [
        Card(
          elevation: 4,
          child: Padding(
            padding: EdgeInsets.all(16),
            child: Column(
              crossAxisAlignment: CrossAxisAlignment.start,
              children: [
                Text('Signal Strength Distribution', style: TextStyle(fontSize: 18, fontWeight: FontWeight.bold)),
                SizedBox(height: 16),
                Container(
                  height: 200,
                  child: Center(
                    child: Text('Signal distribution chart would be shown here\n\nExcellent: ${data.where((d) => d.rssi >= -70).length}\nGood: ${data.where((d) => d.rssi >= -85 && d.rssi < -70).length}\nFair: ${data.where((d) => d.rssi >= -100 && d.rssi < -85).length}\nPoor: ${data.where((d) => d.rssi < -100).length}', 
                      textAlign: TextAlign.center,
                    ),
                  ),
                ),
              ],
            ),
          ),
        ),
        SizedBox(height: 16),
        
        Card(
          elevation: 4,
          child: Padding(
            padding: EdgeInsets.all(16),
            child: Column(
              crossAxisAlignment: CrossAxisAlignment.start,
              children: [
                Text('Operator Performance', style: TextStyle(fontSize: 18, fontWeight: FontWeight.bold)),
                SizedBox(height: 16),
                _buildOperatorPerformance(data),
              ],
            ),
          ),
        ),
      ],
    );
  }

  Widget _buildQualityTab(List<SignalData> data) {
    final qualityScore = _calculateQualityScore(data);
    
    return ListView(
      padding: EdgeInsets.all(16),
      children: [
        Card(
          elevation: 4,
          child: Padding(
            padding: EdgeInsets.all(16),
            child: Column(
              crossAxisAlignment: CrossAxisAlignment.start,
              children: [
                Text('Network Quality Score', style: TextStyle(fontSize: 18, fontWeight: FontWeight.bold)),
                SizedBox(height: 16),
                _buildQualityScore(qualityScore),
                SizedBox(height: 20),
                Text('Quality Metrics:', style: TextStyle(fontWeight: FontWeight.bold)),
                SizedBox(height: 12),
                _buildQualityMetrics(data),
              ],
            ),
          ),
        ),
        SizedBox(height: 16),
        
        Card(
          elevation: 4,
          child: Padding(
            padding: EdgeInsets.all(16),
            child: Column(
              crossAxisAlignment: CrossAxisAlignment.start,
              children: [
                Text('Recommendations', style: TextStyle(fontSize: 18, fontWeight: FontWeight.bold)),
                SizedBox(height: 16),
                _buildRecommendations(data, qualityScore),
              ],
            ),
          ),
        ),
      ],
    );
  }

  // Analysis Components
  Widget _buildSignalStrengthAnalysis(List<SignalData> data) {
    final excellent = data.where((d) => d.rssi >= -70).length;
    final good = data.where((d) => d.rssi >= -85 && d.rssi < -70).length;
    final fair = data.where((d) => d.rssi >= -100 && d.rssi < -85).length;
    final poor = data.where((d) => d.rssi < -100).length;
    final total = data.length;

    return Column(
      children: [
        Row(
          mainAxisAlignment: MainAxisAlignment.spaceAround,
          children: [
            _buildSignalCategory('Excellent', excellent, total, Colors.green),
            _buildSignalCategory('Good', good, total, Colors.lightGreen),
            _buildSignalCategory('Fair', fair, total, Colors.orange),
            _buildSignalCategory('Poor', poor, total, Colors.red),
          ],
        ),
        SizedBox(height: 16),
        LinearProgressIndicator(
          value: total > 0 ? (excellent + good) / total : 0,
          backgroundColor: Colors.grey[300],
          valueColor: AlwaysStoppedAnimation(Colors.green),
          minHeight: 8,
        ),
        SizedBox(height: 8),
        Text('${total > 0 ? ((excellent + good) / total * 100).toStringAsFixed(1) : "0"}% Good or Excellent Signal',
            style: TextStyle(fontSize: 12, color: Colors.grey)),
      ],
    );
  }

  Widget _buildSignalCategory(String label, int count, int total, Color color) {
    final percentage = total == 0 ? 0 : (count / total * 100);
    return Column(
      children: [
        Container(
          width: 40,
          height: 40,
          decoration: BoxDecoration(
            color: color,
            shape: BoxShape.circle,
          ),
          child: Center(
            child: Text(count.toString(), 
                style: TextStyle(color: Colors.white, fontWeight: FontWeight.bold)),
          ),
        ),
        SizedBox(height: 4),
        Text(label, style: TextStyle(fontSize: 10)),
        Text('${percentage.toStringAsFixed(1)}%', 
            style: TextStyle(fontSize: 10, color: Colors.grey)),
      ],
    );
  }

  Widget _buildNetworkTypeAnalysis(List<SignalData> data) {
    final networkGroups = _groupByNetworkType(data);
    
    return Column(
      children: networkGroups.entries.map((entry) {
        final percentage = data.isEmpty ? 0 : (entry.value.length / data.length * 100);
        return Padding(
          padding: EdgeInsets.symmetric(vertical: 8),
          child: Row(
            children: [
              Expanded(flex: 2, child: Text(entry.key)),
              Expanded(
                flex: 3,
                child: LinearProgressIndicator(
                  value: data.isEmpty ? 0 : entry.value.length / data.length,
                  backgroundColor: Colors.grey[200],
                  valueColor: AlwaysStoppedAnimation(_getNetworkColor(entry.key)),
                ),
              ),
              Expanded(flex: 1, child: Text('${percentage.toStringAsFixed(1)}%', textAlign: TextAlign.right)),
            ],
          ),
        );
      }).toList(),
    );
  }

  Widget _buildLocationCoverage(List<SignalData> data) {
    if (data.isEmpty) return Text('No location data available');
    
    final uniqueLocations = data.map((d) => '${d.latitude.toStringAsFixed(4)}-${d.longitude.toStringAsFixed(4)}').toSet();
    final coverageRadius = _calculateCoverageRadius(data);
    
    return Column(
      crossAxisAlignment: CrossAxisAlignment.start,
      children: [
        _buildCoverageMetric('Unique Locations', uniqueLocations.length.toString()),
        _buildCoverageMetric('Coverage Radius', '${coverageRadius.toStringAsFixed(2)} km'),
        _buildCoverageMetric('Data Points', '${data.length} records'),
        SizedBox(height: 12),
        Text('Coverage Area: ${_getCoverageAreaDescription(coverageRadius)}',
            style: TextStyle(fontSize: 12, color: Colors.grey)),
      ],
    );
  }

  Widget _buildCoverageMetric(String label, String value) {
    return Padding(
      padding: EdgeInsets.symmetric(vertical: 4),
      child: Row(
        mainAxisAlignment: MainAxisAlignment.spaceBetween,
        children: [
          Text(label, style: TextStyle(color: Colors.grey)),
          Text(value, style: TextStyle(fontWeight: FontWeight.bold)),
        ],
      ),
    );
  }

  Widget _buildTimeAnalysis(List<SignalData> data) {
    final peakHour = _findPeakHour(data);
    final worstHour = _findWorstHour(data);
    
    return Column(
      children: [
        _buildTimeAnalysisRow('📊 Best Performance', '${peakHour.hour}:00', 'Avg: ${peakHour.avgRssi.round()} dBm'),
        _buildTimeAnalysisRow('⚠️ Worst Performance', '${worstHour.hour}:00', 'Avg: ${worstHour.avgRssi.round()} dBm'),
        _buildTimeAnalysisRow('🕒 Recommended Usage', _getRecommendedTime(peakHour, worstHour), 'Based on signal analysis'),
      ],
    );
  }

  Widget _buildTimeAnalysisRow(String title, String value, String subtitle) {
    return ListTile(
      title: Text(title),
      trailing: Column(
        crossAxisAlignment: CrossAxisAlignment.end,
        mainAxisSize: MainAxisSize.min,
        children: [
          Text(value, style: TextStyle(fontWeight: FontWeight.bold)),
          Text(subtitle, style: TextStyle(fontSize: 12, color: Colors.grey)),
        ],
      ),
    );
  }

  Widget _buildOperatorPerformance(List<SignalData> data) {
    final operatorStats = _stats['operatorStats'] as Map<String, dynamic>? ?? {};
    
    if (operatorStats.isEmpty) {
      return Text('No operator data available');
    }

    return Column(
      children: operatorStats.entries.map((entry) {
        final stats = entry.value as Map<String, dynamic>;
        return _buildOperatorPerformanceRow(entry.key, stats);
      }).toList(),
    );
  }

  Widget _buildOperatorPerformanceRow(String operator, Map<String, dynamic> stats) {
    return ListTile(
      leading: CircleAvatar(
        backgroundColor: _getOperatorColor(operator),
        child: Text(operator[0], style: TextStyle(color: Colors.white)),
      ),
      title: Text(operator),
      subtitle: Text('${stats['count']} records • ${stats['avgDownload']} Mbps'),
      trailing: Chip(
        label: Text('${stats['avgRssi']} dBm'),
        backgroundColor: _getSignalColor(_getSignalLevel(stats['avgRssi'])).withOpacity(0.2),
        labelStyle: TextStyle(color: _getSignalColor(_getSignalLevel(stats['avgRssi']))),
      ),
    );
  }

  Widget _buildQualityScore(double score) {
    return Column(
      children: [
        Stack(
          alignment: Alignment.center,
          children: [
            CircularProgressIndicator(
              value: score / 100,
              strokeWidth: 12,
              backgroundColor: Colors.grey[200],
              valueColor: AlwaysStoppedAnimation(_getScoreColor(score)),
            ),
            Column(
              children: [
                Text(
                  '${score.toStringAsFixed(1)}',
                  style: TextStyle(fontSize: 24, fontWeight: FontWeight.bold),
                ),
                Text(
                  '/100',
                  style: TextStyle(fontSize: 14, color: Colors.grey),
                ),
              ],
            ),
          ],
        ),
        SizedBox(height: 16),
        Text(
          _getScoreDescription(score),
          style: TextStyle(fontSize: 16, color: _getScoreColor(score)),
          textAlign: TextAlign.center,
        ),
      ],
    );
  }

  Widget _buildQualityMetrics(List<SignalData> data) {
    final metrics = _calculateQualityMetrics(data);
    
    return Column(
      children: [
        _buildQualityMetricRow('Signal Strength', '${metrics['signal']?.toStringAsFixed(1) ?? '0'}%', _getMetricColor(metrics['signal'] ?? 0)),
        _buildQualityMetricRow('Network Stability', '${metrics['stability']?.toStringAsFixed(1) ?? '0'}%', _getMetricColor(metrics['stability'] ?? 0)),
        _buildQualityMetricRow('Coverage Quality', '${metrics['coverage']?.toStringAsFixed(1) ?? '0'}%', _getMetricColor(metrics['coverage'] ?? 0)),
        _buildQualityMetricRow('Data Consistency', '${metrics['consistency']?.toStringAsFixed(1) ?? '0'}%', _getMetricColor(metrics['consistency'] ?? 0)),
      ],
    );
  }

  Widget _buildQualityMetricRow(String label, String value, Color color) {
    return Padding(
      padding: EdgeInsets.symmetric(vertical: 8),
      child: Row(
        children: [
          Expanded(child: Text(label)),
          Container(
            width: 60,
            padding: EdgeInsets.symmetric(horizontal: 8, vertical: 4),
            decoration: BoxDecoration(
              color: color.withOpacity(0.2),
              borderRadius: BorderRadius.circular(12),
            ),
            child: Text(value, 
              style: TextStyle(color: color, fontWeight: FontWeight.bold),
              textAlign: TextAlign.center,
            ),
          ),
        ],
      ),
    );
  }

  Widget _buildRecommendations(List<SignalData> data, double qualityScore) {
    final recommendations = _generateRecommendations(data, qualityScore);
    
    return Column(
      children: recommendations.map((rec) => ListTile(
        leading: Icon(rec.icon, color: rec.color),
        title: Text(rec.title),
        subtitle: Text(rec.description),
      )).toList(),
    );
  }

  // Helper Methods
  Map<String, List<SignalData>> _groupByNetworkType(List<SignalData> data) {
    final Map<String, List<SignalData>> groups = {};
    for (var item in data) {
      groups.putIfAbsent(item.networkType, () => []).add(item);
    }
    return groups;
  }

  double _calculateCoverageRadius(List<SignalData> data) {
    if (data.length < 2) return 0.0;
    
    final lats = data.map((d) => d.latitude).toList();
    final lngs = data.map((d) => d.longitude).toList();
    
    final centerLat = lats.reduce((a, b) => a + b) / lats.length;
    final centerLng = lngs.reduce((a, b) => a + b) / lngs.length;
    
    double maxDistance = 0;
    for (var point in data) {
      final distance = _calculateDistance(centerLat, centerLng, point.latitude, point.longitude);
      if (distance > maxDistance) maxDistance = distance;
    }
    
    return maxDistance;
  }

  double _calculateDistance(double lat1, double lng1, double lat2, double lng2) {
    const earthRadius = 6371.0; // kilometers
    
    final dLat = _toRadians(lat2 - lat1);
    final dLng = _toRadians(lng2 - lng1);
    
    final a = sin(dLat/2) * sin(dLat/2) +
             cos(_toRadians(lat1)) * cos(_toRadians(lat2)) *
             sin(dLng/2) * sin(dLng/2);
    final c = 2 * atan2(sqrt(a), sqrt(1-a));
    
    return earthRadius * c;
  }

  double _toRadians(double degrees) {
    return degrees * pi / 180;
  }

  String _getCoverageAreaDescription(double radius) {
    if (radius < 1) return 'Very Localized (${(radius * 1000).toStringAsFixed(0)}m)';
    if (radius < 5) return 'Small Area (${radius.toStringAsFixed(1)}km)';
    if (radius < 20) return 'Medium Area (${radius.toStringAsFixed(1)}km)';
    return 'Large Area (${radius.toStringAsFixed(1)}km)';
  }

  _HourlyPerformance _findPeakHour(List<SignalData> data) {
    final hourlyData = _groupDataByHour(data);
    var bestHour = 12;
    var bestAvg = -85.0;
    
    if (hourlyData.isNotEmpty) {
      hourlyData.forEach((hour, avg) {
        if (avg > bestAvg) {
          bestAvg = avg;
          bestHour = hour;
        }
      });
    }
    
    return _HourlyPerformance(bestHour, bestAvg);
  }

  _HourlyPerformance _findWorstHour(List<SignalData> data) {
    final hourlyData = _groupDataByHour(data);
    var worstHour = 12;
    var worstAvg = -85.0;
    
    if (hourlyData.isNotEmpty) {
      worstAvg = hourlyData.values.first;
      hourlyData.forEach((hour, avg) {
        if (avg < worstAvg) {
          worstAvg = avg;
          worstHour = hour;
        }
      });
    }
    
    return _HourlyPerformance(worstHour, worstAvg);
  }

  Map<int, double> _groupDataByHour(List<SignalData> data) {
    final Map<int, List<int>> hourlyRssi = {};
    for (var item in data) {
      final hour = item.timestamp.hour;
      hourlyRssi.putIfAbsent(hour, () => []).add(item.rssi);
    }
    
    final Map<int, double> hourlyAvg = {};
    hourlyRssi.forEach((hour, rssiList) {
      hourlyAvg[hour] = rssiList.reduce((a, b) => a + b) / rssiList.length;
    });
    
    return hourlyAvg;
  }

  String _getRecommendedTime(_HourlyPerformance peak, _HourlyPerformance worst) {
    return '${peak.hour}:00 - ${(peak.hour + 2) % 24}:00';
  }

  double _calculateQualityScore(List<SignalData> data) {
    if (data.isEmpty) return 0;
    
    final metrics = _calculateQualityMetrics(data);
    final signal = metrics['signal'] ?? 0;
    final stability = metrics['stability'] ?? 0;
    final coverage = metrics['coverage'] ?? 0;
    final consistency = metrics['consistency'] ?? 0;
    
    return (signal + stability + coverage + consistency) / 4;
  }

  Map<String, double> _calculateQualityMetrics(List<SignalData> data) {
    if (data.isEmpty) return {'signal': 0.0, 'stability': 0.0, 'coverage': 0.0, 'consistency': 0.0};
    
    // Signal strength score (0-100)
    final avgRssi = data.map((d) => d.rssi).reduce((a, b) => a + b) / data.length;
    final signalScore = ((avgRssi + 120) / 70 * 100).clamp(0, 100).toDouble();
    
    // Stability score (0-100) - based on standard deviation
    final mean = avgRssi;
    final variance = data.map((d) => pow(d.rssi - mean, 2)).reduce((a, b) => a + b) / data.length;
    final stdDev = sqrt(variance);
    final stabilityScore = (100 - (stdDev / 10 * 100)).clamp(0, 100).toDouble();
    
    // Coverage score (0-100) - based on excellent signal percentage
    final excellentCount = data.where((d) => d.rssi >= -70).length;
    final coverageScore = (excellentCount / data.length * 100).clamp(0, 100).toDouble();
    
    // Consistency score (0-100) - based on network type consistency
    final networkGroups = _groupByNetworkType(data);
    final mainNetwork = networkGroups.entries.reduce((a, b) => a.value.length > b.value.length ? a : b).key;
    final consistencyScore = (data.where((d) => d.networkType == mainNetwork).length / data.length * 100).toDouble();
    
    return {
      'signal': signalScore,
      'stability': stabilityScore,
      'coverage': coverageScore,
      'consistency': consistencyScore,
    };
  }

  List<_Recommendation> _generateRecommendations(List<SignalData> data, double qualityScore) {
    final recommendations = <_Recommendation>[];
    
    if (qualityScore < 60) {
      recommendations.add(_Recommendation(
        Icons.warning,
        'Improve Signal Quality',
        'Consider moving to areas with better network coverage',
        Colors.orange
      ));
    }
    
    if (data.any((d) => d.rssi <= -100)) {
      recommendations.add(_Recommendation(
        Icons.signal_cellular_connected_no_internet_0_bar,
        'Poor Signal Areas Detected',
        '${data.where((d) => d.rssi <= -100).length} records show very poor signal',
        Colors.red
      ));
    }
    
    final networkTypes = _groupByNetworkType(data);
    if (networkTypes.length > 1) {
      recommendations.add(_Recommendation(
        Icons.swap_horiz,
        'Multiple Networks Available',
        'Consider switching between ${networkTypes.keys.join(', ')} for better performance',
        Colors.blue
      ));
    }
    
    if (recommendations.isEmpty) {
      recommendations.add(_Recommendation(
        Icons.check_circle,
        'Good Network Quality',
        'Your current network performance is satisfactory',
        Colors.green
      ));
    }
    
    return recommendations;
  }

  Color _getNetworkColor(String networkType) {
    switch (networkType) {
      case '4G': return Colors.blue;
      case 'WiFi': return Colors.green;
      case 'Ethernet': return Colors.orange;
      default: return Colors.grey;
    }
  }

  Color _getOperatorColor(String operator) {
    final colors = {
      'Airtel': Colors.red,
      'Jio': Colors.pink,
      'VI': Colors.orange,
      'BSNL': Colors.blue,
      'Unknown': Colors.grey,
    };
    return colors[operator] ?? Colors.purple;
  }

  Color _getSignalColor(String level) {
    switch (level) {
      case 'Excellent': return Colors.green;
      case 'Good': return Colors.lightGreen;
      case 'Fair': return Colors.orange;
      case 'Poor': return Colors.red;
      default: return Colors.grey;
    }
  }

  String _getSignalLevel(int rssi) {
    if (rssi >= -70) return 'Excellent';
    if (rssi >= -85) return 'Good';
    if (rssi >= -100) return 'Fair';
    return 'Poor';
  }

  Color _getScoreColor(double score) {
    if (score >= 80) return Colors.green;
    if (score >= 60) return Colors.lightGreen;
    if (score >= 40) return Colors.orange;
    return Colors.red;
  }

  Color _getMetricColor(double value) {
    if (value >= 80) return Colors.green;
    if (value >= 60) return Colors.lightGreen;
    if (value >= 40) return Colors.orange;
    return Colors.red;
  }

  String _getScoreDescription(double score) {
    if (score >= 80) return 'Excellent Network Quality';
    if (score >= 60) return 'Good Network Quality';
    if (score >= 40) return 'Fair Network Quality';
    return 'Poor Network Quality - Needs Improvement';
  }
}

// Supporting Classes for Data Analysis
class _HourlyPerformance {
  final int hour;
  final double avgRssi;
  
  _HourlyPerformance(this.hour, this.avgRssi);
}

class _Recommendation {
  final IconData icon;
  final String title;
  final String description;
  final Color color;
  
  _Recommendation(this.icon, this.title, this.description, this.color);
}

// Enhanced Operator Comparison Screen
class OperatorComparisonScreen extends StatefulWidget {
  final SignalService signalService;

  OperatorComparisonScreen({required this.signalService});

  @override
  _OperatorComparisonScreenState createState() => _OperatorComparisonScreenState();
}

class _OperatorComparisonScreenState extends State<OperatorComparisonScreen> {
  Map<String, dynamic> _operatorStats = {};
  String _selectedMetric = 'Signal Strength';
  List<String> _metrics = ['Signal Strength', 'Download Speed', 'Upload Speed', 'Coverage', 'Stability'];
  bool _isLoading = true;

  @override
  void initState() {
    super.initState();
    _loadOperatorStats();
  }

  Future<void> _loadOperatorStats() async {
    try {
      final stats = await widget.signalService.getStatistics();
      
      setState(() {
        _operatorStats = stats['operatorStats'] as Map<String, dynamic>? ?? {};
        _isLoading = false;
      });
    } catch (e) {
      print("Error loading operator stats: $e");
      setState(() {
        _isLoading = false;
      });
    }
  }

  @override
  Widget build(BuildContext context) {
    return Scaffold(
      appBar: AppBar(
        title: Text('Operator Comparison'),
        backgroundColor: Colors.teal[700],
      ),
      body: _isLoading 
          ? _buildLoadingState()
          : _operatorStats.isEmpty 
            ? _buildEmptyState() 
            : _buildComparisonContent(),
    );
  }

  Widget _buildLoadingState() {
    return Center(
      child: Column(
        mainAxisAlignment: MainAxisAlignment.center,
        children: [
          CircularProgressIndicator(),
          SizedBox(height: 16),
          Text('Loading operator data...'),
        ],
      ),
    );
  }

  Widget _buildEmptyState() {
    return Center(
      child: Column(
        mainAxisAlignment: MainAxisAlignment.center,
        children: [
          Icon(Icons.compare, size: 64, color: Colors.grey),
          SizedBox(height: 16),
          Text(
            'No Operator Data Available',
            style: TextStyle(fontSize: 18, color: Colors.grey),
          ),
          SizedBox(height: 8),
          Text(
            'Collect data from different operators on the Home screen',
            style: TextStyle(color: Colors.grey),
            textAlign: TextAlign.center,
          ),
        ],
      ),
    );
  }

  Widget _buildComparisonContent() {
    final operators = _operatorStats.keys.toList();
    final rankedOperators = _getRankedOperators();
    
    return Column(
      children: [
        // Metric Selector
        Padding(
          padding: EdgeInsets.all(16),
          child: Card(
            child: Padding(
              padding: EdgeInsets.all(12),
              child: Column(
                crossAxisAlignment: CrossAxisAlignment.start,
                children: [
                  Text('Comparison Metric:', style: TextStyle(fontWeight: FontWeight.bold)),
                  SizedBox(height: 8),
                  Wrap(
                    spacing: 8,
                    runSpacing: 8,
                    children: _metrics.map((metric) => ChoiceChip(
                      label: Text(metric),
                      selected: _selectedMetric == metric,
                      onSelected: (selected) {
                        setState(() {
                          _selectedMetric = metric;
                        });
                      },
                    )).toList(),
                  ),
                ],
              ),
            ),
          ),
        ),
        
        // Operator Rankings
        Padding(
          padding: EdgeInsets.symmetric(horizontal: 16),
          child: Card(
            child: Padding(
              padding: EdgeInsets.all(12),
              child: Column(
                crossAxisAlignment: CrossAxisAlignment.start,
                children: [
                  Text('Operator Rankings', style: TextStyle(fontSize: 16, fontWeight: FontWeight.bold)),
                  SizedBox(height: 8),
                  _buildRankingChips(rankedOperators),
                ],
              ),
            ),
          ),
        ),
        
        // Comparison Dashboard
        Expanded(
          child: ListView(
            padding: EdgeInsets.all(16),
            children: [
              _buildComparisonChart(),
              SizedBox(height: 20),
              _buildDetailedComparison(),
              SizedBox(height: 20),
              _buildPerformanceSummary(),
            ],
          ),
        ),
      ],
    );
  }

  Widget _buildRankingChips(List<String> rankedOperators) {
    return Wrap(
      spacing: 8,
      runSpacing: 8,
      children: rankedOperators.asMap().entries.map((entry) {
        final rank = entry.key + 1;
        final operator = entry.value;
        return Chip(
          label: Text('$rank. $operator'),
          backgroundColor: _getRankColor(rank),
          labelStyle: TextStyle(color: Colors.white, fontWeight: FontWeight.bold),
        );
      }).toList(),
    );
  }

  Widget _buildComparisonChart() {
    return Card(
      elevation: 4,
      child: Padding(
        padding: EdgeInsets.all(16),
        child: Column(
          crossAxisAlignment: CrossAxisAlignment.start,
          children: [
            Text(
              'Operator Comparison - $_selectedMetric',
              style: TextStyle(fontSize: 18, fontWeight: FontWeight.bold),
            ),
            SizedBox(height: 16),
            Container(
              height: 300,
              child: _buildComparativeBarChart(),
            ),
            SizedBox(height: 16),
            _buildChartLegend(),
          ],
        ),
      ),
    );
  }

  Widget _buildComparativeBarChart() {
    final operators = _operatorStats.keys.toList();
    if (operators.isEmpty) return Center(child: Text('No data available'));
    
    return ListView(
      scrollDirection: Axis.horizontal,
      children: [
        Padding(
          padding: EdgeInsets.all(16),
          child: Column(
            children: [
              // Chart Bars
              Expanded(
                child: Row(
                  crossAxisAlignment: CrossAxisAlignment.end,
                  children: operators.asMap().entries.map((entry) {
                    final operator = entry.value;
                    final stats = _operatorStats[operator] as Map<String, dynamic>;
                    final value = _getMetricValue(stats, _selectedMetric);
                    final maxValue = _getMaxValueForMetric(_selectedMetric);
                    final barHeight = maxValue > 0 ? (value / maxValue) * 200 : 0.0;
                    
                    return Padding(
                      padding: EdgeInsets.symmetric(horizontal: 12),
                      child: Column(
                        mainAxisAlignment: MainAxisAlignment.end,
                        children: [
                          Tooltip(
                            message: '${_formatValue(value, _selectedMetric)}\n${stats['count']} records',
                            child: Container(
                              width: 40,
                              height: barHeight,
                              decoration: BoxDecoration(
                                color: _getOperatorColor(operator),
                                borderRadius: BorderRadius.only(
                                  topLeft: Radius.circular(4),
                                  topRight: Radius.circular(4),
                                ),
                                gradient: LinearGradient(
                                  begin: Alignment.bottomCenter,
                                  end: Alignment.topCenter,
                                  colors: [
                                    _getOperatorColor(operator).withOpacity(0.8),
                                    _getOperatorColor(operator),
                                  ],
                                ),
                              ),
                            ),
                          ),
                          SizedBox(height: 8),
                          Container(
                            width: 60,
                            child: Column(
                              children: [
                                Text(
                                  operator,
                                  style: TextStyle(fontSize: 10, fontWeight: FontWeight.bold),
                                  textAlign: TextAlign.center,
                                  maxLines: 2,
                                ),
                                SizedBox(height: 2),
                                Text(
                                  _formatValue(value, _selectedMetric),
                                  style: TextStyle(fontSize: 9, fontWeight: FontWeight.bold),
                                ),
                              ],
                            ),
                          ),
                        ],
                      ),
                    );
                  }).toList(),
                ),
              ),
            ],
          ),
        ),
      ],
    );
  }

  Widget _buildChartLegend() {
    return Row(
      mainAxisAlignment: MainAxisAlignment.spaceAround,
      children: _operatorStats.keys.map((operator) {
        return Row(
          children: [
            Container(
              width: 12,
              height: 12,
              decoration: BoxDecoration(
                color: _getOperatorColor(operator),
                shape: BoxShape.circle,
              ),
            ),
            SizedBox(width: 4),
            Text(operator, style: TextStyle(fontSize: 12)),
          ],
        );
      }).toList(),
    );
  }

  Widget _buildDetailedComparison() {
    return Card(
      elevation: 4,
      child: Padding(
        padding: EdgeInsets.all(16),
        child: Column(
          crossAxisAlignment: CrossAxisAlignment.start,
          children: [
            Text('Detailed Comparison', style: TextStyle(fontSize: 18, fontWeight: FontWeight.bold)),
            SizedBox(height: 16),
            SingleChildScrollView(
              scrollDirection: Axis.horizontal,
              child: DataTable(
                columnSpacing: 20,
                columns: [
                  DataColumn(label: Text('Metric', style: TextStyle(fontWeight: FontWeight.bold))),
                  ..._operatorStats.keys.map((operator) => DataColumn(
                    label: Container(
                      width: 80,
                      child: Text(operator, 
                        style: TextStyle(fontWeight: FontWeight.bold),
                        textAlign: TextAlign.center,
                      ),
                    ),
                  )).toList(),
                ],
                rows: _buildComparisonRows(),
              ),
            ),
          ],
        ),
      ),
    );
  }

  List<DataRow> _buildComparisonRows() {
    return [
      _buildComparisonRow('Signal Strength (dBm)', (stats) => '${stats['avgRssi']} dBm'),
      _buildComparisonRow('Download Speed', (stats) => '${stats['avgDownload']} Mbps'),
      _buildComparisonRow('Upload Speed', (stats) => '${stats['avgUpload']} Mbps'),
      _buildComparisonRow('Data Points', (stats) => '${stats['count']}'),
      _buildComparisonRow('Signal Quality', (stats) => _calculateSignalQuality(stats['avgRssi'])),
      _buildComparisonRow('Performance Score', (stats) => _calculatePerformanceScore(stats).toStringAsFixed(1)),
    ];
  }

  DataRow _buildComparisonRow(String label, String Function(Map<String, dynamic>) valueGetter) {
    return DataRow(
      cells: [
        DataCell(Text(label, style: TextStyle(fontWeight: FontWeight.bold))),
        ..._operatorStats.keys.map((operator) {
          final stats = _operatorStats[operator] as Map<String, dynamic>;
          final value = valueGetter(stats);
          return DataCell(
            Container(
              width: 80,
              child: Text(value, 
                style: TextStyle(fontWeight: FontWeight.w500),
                textAlign: TextAlign.center,
              ),
            ),
          );
        }).toList(),
      ],
    );
  }

  Widget _buildPerformanceSummary() {
    final rankedOperators = _getRankedOperators();
    final bestOperator = rankedOperators.isNotEmpty ? rankedOperators.first : 'N/A';
    final bestStats = bestOperator != 'N/A' ? _operatorStats[bestOperator] as Map<String, dynamic> : null;
    
    return Card(
      elevation: 4,
      child: Padding(
        padding: EdgeInsets.all(16),
        child: Column(
          crossAxisAlignment: CrossAxisAlignment.start,
          children: [
            Text('Performance Summary', style: TextStyle(fontSize: 18, fontWeight: FontWeight.bold)),
            SizedBox(height: 16),
            if (bestOperator != 'N/A' && bestStats != null) ...[
              _buildSummaryItem('🏆 Best Performer', bestOperator, _getOperatorColor(bestOperator)),
              _buildSummaryItem('📶 Average Signal', '${bestStats['avgRssi']} dBm', Colors.green),
              _buildSummaryItem('⬇️ Download Speed', '${bestStats['avgDownload']} Mbps', Colors.blue),
              _buildSummaryItem('⬆️ Upload Speed', '${bestStats['avgUpload']} Mbps', Colors.orange),
              _buildSummaryItem('📊 Data Reliability', '${bestStats['count']} records', Colors.purple),
              SizedBox(height: 16),
              _buildRecommendation(bestOperator, bestStats),
            ] else ...[
              Text('No performance data available', style: TextStyle(color: Colors.grey)),
            ],
          ],
        ),
      ),
    );
  }

  Widget _buildSummaryItem(String label, String value, Color color) {
    return Padding(
      padding: EdgeInsets.symmetric(vertical: 8),
      child: Row(
        children: [
          Expanded(child: Text(label, style: TextStyle(color: Colors.grey))),
          Container(
            padding: EdgeInsets.symmetric(horizontal: 12, vertical: 6),
            decoration: BoxDecoration(
              color: color.withOpacity(0.1),
              borderRadius: BorderRadius.circular(16),
              border: Border.all(color: color.withOpacity(0.3)),
            ),
            child: Text(value, 
              style: TextStyle(color: color, fontWeight: FontWeight.bold),
            ),
          ),
        ],
      ),
    );
  }

  Widget _buildRecommendation(String bestOperator, Map<String, dynamic> bestStats) {
    final score = _calculatePerformanceScore(bestStats);
    String recommendation;
    Color color;
    
    if (score >= 80) {
      recommendation = 'Excellent choice for all purposes';
      color = Colors.green;
    } else if (score >= 60) {
      recommendation = 'Good for general use';
      color = Colors.lightGreen;
    } else if (score >= 40) {
      recommendation = 'Adequate for basic needs';
      color = Colors.orange;
    } else {
      recommendation = 'Consider alternative options';
      color = Colors.red;
    }
    
    return Container(
      padding: EdgeInsets.all(12),
      decoration: BoxDecoration(
        color: color.withOpacity(0.1),
        borderRadius: BorderRadius.circular(8),
        border: Border.all(color: color.withOpacity(0.3)),
      ),
      child: Row(
        children: [
          Icon(Icons.lightbulb, color: color),
          SizedBox(width: 12),
          Expanded(
            child: Column(
              crossAxisAlignment: CrossAxisAlignment.start,
              children: [
                Text('Recommendation', style: TextStyle(fontWeight: FontWeight.bold, color: color)),
                SizedBox(height: 4),
                Text(recommendation, style: TextStyle(fontSize: 12)),
              ],
            ),
          ),
        ],
      ),
    );
  }

  // Helper Methods
  double _getMetricValue(Map<String, dynamic> stats, String metric) {
    switch (metric) {
      case 'Signal Strength':
        return (stats['avgRssi'] as int).toDouble();
      case 'Download Speed':
        return double.parse(stats['avgDownload']);
      case 'Upload Speed':
        return double.parse(stats['avgUpload']);
      case 'Coverage':
        return (stats['count'] as int).toDouble();
      case 'Stability':
        return (stats['avgRssi'] as int).toDouble(); // Simplified stability
      default:
        return 0.0;
    }
  }

  double _getMaxValueForMetric(String metric) {
    final values = _operatorStats.values.map((stats) => _getMetricValue(stats as Map<String, dynamic>, metric)).toList();
    if (values.isEmpty) return 1.0;
    final maxValue = values.reduce((a, b) => a > b ? a : b);
    return maxValue * 1.2; // Add 20% padding
  }

  String _formatValue(double value, String metric) {
    switch (metric) {
      case 'Signal Strength':
        return '${value.round()}';
      case 'Download Speed':
      case 'Upload Speed':
        return value.toStringAsFixed(1);
      case 'Coverage':
        return '${value.round()}';
      case 'Stability':
        return '${value.round()}';
      default:
        return value.toStringAsFixed(1);
    }
  }

  Color _getOperatorColor(String operator) {
    final colors = {
      'Airtel': Colors.red,
      'Jio': Colors.pink,
      'VI': Colors.orange,
      'BSNL': Colors.blue,
      'Unknown': Colors.grey,
    };
    return colors[operator] ?? Colors.purple;
  }

  Color _getRankColor(int rank) {
    switch (rank) {
      case 1: return Colors.amber;
      case 2: return Colors.grey;
      case 3: return Colors.brown;
      default: return Colors.blue;
    }
  }

  List<String> _getRankedOperators() {
    final operators = _operatorStats.keys.toList();
    operators.sort((a, b) {
      final statsA = _operatorStats[a] as Map<String, dynamic>;
      final statsB = _operatorStats[b] as Map<String, dynamic>;
      final scoreA = _calculatePerformanceScore(statsA);
      final scoreB = _calculatePerformanceScore(statsB);
      return scoreB.compareTo(scoreA);
    });
    return operators;
  }

  String _calculateSignalQuality(int avgRssi) {
    if (avgRssi >= -70) return 'Excellent';
    if (avgRssi >= -85) return 'Good';
    if (avgRssi >= -100) return 'Fair';
    return 'Poor';
  }

  double _calculatePerformanceScore(Map<String, dynamic> stats) {
    // Calculate a composite score based on multiple metrics
    final signalScore = ((stats['avgRssi'] as int) + 120) / 70 * 50; // 50 points max
    final downloadScore = (double.parse(stats['avgDownload']) / 20 * 30); // 30 points max
    final coverageScore = (stats['count'] as int) / 100 * 20; // 20 points max, assuming 100 records is excellent
    
    return (signalScore + downloadScore + coverageScore).clamp(0, 100).toDouble();
  }
}

// Settings Screen
class SettingsScreen extends StatefulWidget {
  final SignalService signalService;

  SettingsScreen({required this.signalService});

  @override
  _SettingsScreenState createState() => _SettingsScreenState();
}

class _SettingsScreenState extends State<SettingsScreen> {
  int _monitoringInterval = 5;
  bool _autoExport = false;
  bool _notifications = true;

  @override
  Widget build(BuildContext context) {
    return Scaffold(
      appBar: AppBar(
        title: Text('Settings'),
        backgroundColor: Colors.orange[700],
      ),
      body: ListView(
        padding: EdgeInsets.all(16),
        children: [
          // Monitoring Settings
          Card(
            elevation: 4,
            child: Padding(
              padding: EdgeInsets.all(16),
              child: Column(
                crossAxisAlignment: CrossAxisAlignment.start,
                children: [
                  Text('Monitoring Settings', style: TextStyle(fontSize: 18, fontWeight: FontWeight.bold)),
                  SizedBox(height: 16),
                  _buildSettingRow(
                    'Monitoring Interval',
                    '$_monitoringInterval seconds',
                    Icons.timer,
                    _buildIntervalSlider(),
                  ),
                  SizedBox(height: 16),
                  _buildSwitchSetting(
                    'Auto Export',
                    'Automatically export data daily',
                    Icons.save_alt,
                    _autoExport,
                    (value) => setState(() => _autoExport = value),
                  ),
                ],
              ),
            ),
          ),
          SizedBox(height: 16),
          
          // Notification Settings
          Card(
            elevation: 4,
            child: Padding(
              padding: EdgeInsets.all(16),
              child: Column(
                crossAxisAlignment: CrossAxisAlignment.start,
                children: [
                  Text('Notifications', style: TextStyle(fontSize: 18, fontWeight: FontWeight.bold)),
                  SizedBox(height: 16),
                  _buildSwitchSetting(
                    'Enable Notifications',
                    'Get alerts for poor signal',
                    Icons.notifications,
                    _notifications,
                    (value) => setState(() => _notifications = value),
                  ),
                ],
              ),
            ),
          ),
          SizedBox(height: 16),
          
          // Data Management
          Card(
            elevation: 4,
            child: Padding(
              padding: EdgeInsets.all(16),
              child: Column(
                crossAxisAlignment: CrossAxisAlignment.start,
                children: [
                  Text('Data Management', style: TextStyle(fontSize: 18, fontWeight: FontWeight.bold)),
                  SizedBox(height: 16),
                  _buildActionSetting(
                    'Export All Data',
                    'Download complete dataset as CSV',
                    Icons.download,
                    _exportAllData,
                  ),
                  SizedBox(height: 12),
                  _buildActionSetting(
                    'Clear All Data',
                    'Permanently delete all collected data',
                    Icons.delete,
                    _clearAllData,
                    isDestructive: true,
                  ),
                ],
              ),
            ),
          ),
          SizedBox(height: 16),
          
          // App Info
          Card(
            elevation: 4,
            child: Padding(
              padding: EdgeInsets.all(16),
              child: Column(
                crossAxisAlignment: CrossAxisAlignment.start,
                children: [
                  Text('App Information', style: TextStyle(fontSize: 18, fontWeight: FontWeight.bold)),
                  SizedBox(height: 16),
                  _buildInfoRow('Version', '1.0.0'),
                  _buildInfoRow('Build Date', '${DateTime.now().toString().substring(0, 10)}'),
                  _buildInfoRow('Data Records', '${widget.signalService.getHistoricalData().then((data) => data.length).toString()}'),
                ],
              ),
            ),
          ),
        ],
      ),
    );
  }

  Widget _buildSettingRow(String title, String subtitle, IconData icon, Widget control) {
    return Row(
      children: [
        Icon(icon, size: 24, color: Colors.blue),
        SizedBox(width: 16),
        Expanded(
          child: Column(
            crossAxisAlignment: CrossAxisAlignment.start,
            children: [
              Text(title, style: TextStyle(fontWeight: FontWeight.bold)),
              SizedBox(height: 2),
              Text(subtitle, style: TextStyle(fontSize: 12, color: Colors.grey)),
            ],
          ),
        ),
        SizedBox(width: 16),
        control,
      ],
    );
  }

  Widget _buildIntervalSlider() {
    return Container(
      width: 150,
      child: Column(
        children: [
          Slider(
            value: _monitoringInterval.toDouble(),
            min: 2,
            max: 30,
            divisions: 14,
            label: '$_monitoringInterval s',
            onChanged: (value) {
              setState(() {
                _monitoringInterval = value.round();
              });
            },
          ),
        ],
      ),
    );
  }

  Widget _buildSwitchSetting(String title, String subtitle, IconData icon, bool value, Function(bool) onChanged) {
    return Row(
      children: [
        Icon(icon, size: 24, color: Colors.blue),
        SizedBox(width: 16),
        Expanded(
          child: Column(
            crossAxisAlignment: CrossAxisAlignment.start,
            children: [
              Text(title, style: TextStyle(fontWeight: FontWeight.bold)),
              SizedBox(height: 2),
              Text(subtitle, style: TextStyle(fontSize: 12, color: Colors.grey)),
            ],
          ),
        ),
        Switch(
          value: value,
          onChanged: onChanged,
        ),
      ],
    );
  }

  Widget _buildActionSetting(String title, String subtitle, IconData icon, Function onTap, {bool isDestructive = false}) {
    return ListTile(
      leading: Icon(icon, color: isDestructive ? Colors.red : Colors.blue),
      title: Text(title, style: TextStyle(fontWeight: FontWeight.bold)),
      subtitle: Text(subtitle, style: TextStyle(fontSize: 12)),
      trailing: Icon(Icons.chevron_right),
      onTap: () => onTap(),
    );
  }

  Widget _buildInfoRow(String label, String value) {
    return Padding(
      padding: EdgeInsets.symmetric(vertical: 8),
      child: Row(
        mainAxisAlignment: MainAxisAlignment.spaceBetween,
        children: [
          Text(label, style: TextStyle(color: Colors.grey)),
          Text(value, style: TextStyle(fontWeight: FontWeight.bold)),
        ],
      ),
    );
  }

  void _exportAllData() {
    widget.signalService.saveCsvToFile();
    ScaffoldMessenger.of(context).showSnackBar(
      SnackBar(content: Text('Data exported successfully!')),
    );
  }

  void _clearAllData() {
    showDialog(
      context: context,
      builder: (BuildContext context) => AlertDialog(
        title: Text('Clear All Data?'),
        content: Text('This action cannot be undone. All collected signal data will be permanently deleted.'),
        actions: [
          TextButton(
            onPressed: () => Navigator.pop(context),
            child: Text('Cancel'),
          ),
          TextButton(
            onPressed: () {
              widget.signalService.deleteAllData();
              Navigator.pop(context);
              ScaffoldMessenger.of(context).showSnackBar(
                SnackBar(content: Text('All data cleared successfully')),
              );
            },
            child: Text('Clear', style: TextStyle(color: Colors.red)),
          ),
        ],
      ),
    );
  }
}