plugins {
    id("com.android.application")
    id("kotlin-android")
    id("dev.flutter.flutter-gradle-plugin")
}

android {
    namespace = "com.example.rural_signal_measurement"
    compileSdk = 34 // Or use flutter.compileSdkVersion
    ndkVersion = flutter.ndkVersion

    compileOptions {
        sourceCompatibility = JavaVersion.VERSION_11
        targetCompatibility = JavaVersion.VERSION_11
    }

    kotlinOptions {
        jvmTarget = JavaVersion.VERSION_11.toString()
    }

    defaultConfig {
        applicationId = "com.example.rural_signal_measurement"
        minSdk = 21 // Required for geolocator
        targetSdk = 33
        versionCode = flutter.versionCode
        versionName = flutter.versionName
        
        // MultiDex support if needed
        multiDexEnabled = true
        
        // Test instrumentation
        testInstrumentationRunner = "androidx.test.runner.AndroidJUnitRunner"
    }

    // Signing configurations
    signingConfigs {
        create("release") {
            // Use your own keystore for release
            // Store these in a secure location
            // storeFile = file("your-keystore.jks")
            // storePassword = "your-password"
            // keyAlias = "your-alias"
            // keyPassword = "your-password"
        }
    }

    buildTypes {
        debug {
            signingConfig = signingConfigs.getByName("debug")
            applicationIdSuffix = ".debug"
            versionNameSuffix = "-debug"
            debuggable = true
        }
        profile {
            signingConfig = signingConfigs.getByName("debug")
            applicationIdSuffix = ".profile"
            versionNameSuffix = "-profile"
            debuggable = false
        }
        release {
            signingConfig = signingConfigs.getByName("debug") // Change to "release" for production
            // Enable code shrinking and obfuscation
            minifyEnabled = true
            shrinkResources = true
            proguardFiles(
                getDefaultProguardFile("proguard-android-optimize.txt"),
                "proguard-rules.pro"
            )
        }
    }

    // APK splits for smaller size
    splits {
        abi {
            enable = true
            reset()
            include("armeabi-v7a", "arm64-v8a", "x86_64")
            universalApk = true
        }
    }

    // Packaging options
    packaging {
        resources {
            excludes += "/META-INF/{AL2.0,LGPL2.1}"
            excludes += "META-INF/atomicfu.kotlin_module"
        }
    }

    // For Java/Kotlin compatibility
    lint {
        disable = listOf("InvalidPackage")
    }
}

flutter {
    source = "../.."
}

// Dependencies
dependencies {
    // MultiDex support
    implementation("androidx.multidex:multidex:2.0.1")
    
    // Google Play Services for location
    implementation("com.google.android.gms:play-services-location:21.0.1")
    
    // Google Maps (optional)
    implementation("com.google.android.gms:play-services-maps:18.1.0")
    
    // Kotlin Coroutines
    implementation("org.jetbrains.kotlinx:kotlinx-coroutines-android:1.7.1")
    
    // For testing
    testImplementation("junit:junit:4.13.2")
    androidTestImplementation("androidx.test:runner:1.5.2")
    androidTestImplementation("androidx.test.espresso:espresso-core:3.5.1")
}