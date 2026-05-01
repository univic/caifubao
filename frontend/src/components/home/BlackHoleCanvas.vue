<script setup lang="ts">
import { ref, onMounted, onUnmounted } from 'vue'
import * as THREE from 'three'

const containerRef = ref<HTMLDivElement | null>(null)
let renderer: THREE.WebGLRenderer | null = null
let scene: THREE.Scene | null = null
let camera: THREE.PerspectiveCamera | null = null
let animationId: number | null = null
let diskMaterial: THREE.ShaderMaterial | null = null

const time = { value: 0 }

const initThree = () => {
  if (!containerRef.value) return

  const width = window.innerWidth
  const height = window.innerHeight * 0.68

  renderer = new THREE.WebGLRenderer({ 
    antialias: true, 
    alpha: false,
    powerPreference: 'high-performance'
  })
  renderer.setSize(width, height)
  renderer.setPixelRatio(Math.min(window.devicePixelRatio, 2))
  renderer.setClearColor(0x000000, 1)
  renderer.toneMapping = THREE.ACESFilmicToneMapping
  renderer.toneMappingExposure = 1.5
  containerRef.value.appendChild(renderer.domElement)

  scene = new THREE.Scene()

  // Camera - more side view for flatter appearance
  camera = new THREE.PerspectiveCamera(50, width / height, 0.1, 2000)
  camera.position.set(20, 2, 18)
  camera.lookAt(2, 0, 0)

  createBlackHole()
  createPhotonSphere()
  createAccretionDisk()
  createGravitationalLensing()
  createStarField()
}

const createBlackHole = () => {
  if (!scene) return

  // Pure black event horizon
  const coreGeometry = new THREE.SphereGeometry(2.0, 64, 64)
  const coreMaterial = new THREE.MeshBasicMaterial({ 
    color: 0x000000
  })
  const core = new THREE.Mesh(coreGeometry, coreMaterial)
  scene.add(core)
}

const createPhotonSphere = () => {
  if (!scene) return

  // Photon sphere - bright thin ring around event horizon
  const photonGeometry = new THREE.TorusGeometry(2.4, 0.06, 8, 128)
  const photonMaterial = new THREE.MeshBasicMaterial({
    color: 0xffeeaa,
    transparent: true,
    opacity: 0.9,
    blending: THREE.AdditiveBlending
  })
  const photonRing = new THREE.Mesh(photonGeometry, photonMaterial)
  photonRing.rotation.x = Math.PI * 0.5
  scene.add(photonRing)

  // Secondary glow ring
  const glowGeometry = new THREE.TorusGeometry(2.5, 0.15, 8, 128)
  const glowMaterial = new THREE.MeshBasicMaterial({
    color: 0xffaa55,
    transparent: true,
    opacity: 0.5,
    blending: THREE.AdditiveBlending
  })
  const glowRing = new THREE.Mesh(glowGeometry, glowMaterial)
  glowRing.rotation.x = Math.PI * 0.5
  scene.add(glowRing)
}

const createAccretionDisk = () => {
  if (!scene) return

  const diskGeometry = new THREE.RingGeometry(2.6, 12, 256, 1)

  diskMaterial = new THREE.ShaderMaterial({
    uniforms: {
      time: time,
      innerR: { value: 2.6 },
      outerR: { value: 12.0 }
    },
    vertexShader: `
      varying vec2 vUv;
      varying vec3 vPos;
      varying float vAngle;
      
      void main() {
        vUv = uv;
        vPos = position;
        vAngle = atan(position.y, position.x);
        gl_Position = projectionMatrix * modelViewMatrix * vec4(position, 1.0);
      }
    `,
    fragmentShader: `
      precision highp float;
      
      uniform float time;
      uniform float innerR;
      uniform float outerR;
      varying vec2 vUv;
      varying vec3 vPos;
      varying float vAngle;
      
      float hash(vec2 p) {
        return fract(sin(dot(p, vec2(12.9898, 78.233))) * 43758.5453);
      }
      
      float noise(vec2 p) {
        vec2 i = floor(p);
        vec2 f = fract(p);
        f = f * f * (3.0 - 2.0 * f);
        return mix(
          mix(hash(i), hash(i + vec2(1.0, 0.0)), f.x),
          mix(hash(i + vec2(0.0, 1.0)), hash(i + vec2(1.0, 1.0)), f.x),
          f.y
        );
      }
      
      void main() {
        float radius = length(vPos.xy);
        float normR = (radius - innerR) / (outerR - innerR);
        
        float angle = vAngle;
        
        // Doppler effect - approaching side (left) much brighter
        float doppler = 0.25 + 1.5 * smoothstep(0.0, 1.0, (-cos(angle) + 1.0) * 0.5);
        
        // Rotation speed varies with radius
        float rotationSpeed = 1.5 / sqrt(radius * 0.15);
        float effectiveAngle = angle - time * rotationSpeed;
        
        // Spiral arms with varying intensity
        float arm1 = sin(effectiveAngle * 3.0 + radius * 0.4) * 0.5 + 0.5;
        float arm2 = sin(effectiveAngle * 6.0 + radius * 0.6 + 1.0) * 0.5 + 0.5;
        float arm3 = sin(effectiveAngle * 9.0 + radius * 0.8 + 2.0) * 0.5 + 0.5;
        
        // Sharp spiral pattern
        float spiral = pow(arm1, 2.0) * 0.6 + pow(arm2, 3.0) * 0.3 + pow(arm3, 4.0) * 0.1;
        
        // Turbulent noise
        float turb = noise(vec2(effectiveAngle * 2.0 + time * 0.1, radius * 1.5)) * 0.3;
        
        // Temperature gradient
        float temp = pow(1.0 - normR, 0.35);
        
        // Colors - Interstellar palette
        vec3 innerColor = vec3(2.0, 1.6, 1.0);
        vec3 midColor = vec3(1.5, 0.8, 0.2);
        vec3 outerColor = vec3(0.9, 0.35, 0.05);
        
        vec3 color;
        if (temp > 0.5) {
          color = mix(midColor, innerColor, (temp - 0.5) * 2.0);
        } else {
          color = mix(outerColor, midColor, temp * 2.0);
        }
        
        // Combined brightness with Doppler
        float baseBrightness = 1.5 + temp * 2.5;
        float brightness = baseBrightness * (0.3 + spiral * 0.9 + turb) * doppler;
        
        // Sharp inner edge glow
        float innerEdge = exp(-normR * 12.0) * 4.0;
        
        // Radial fade
        float edgeFade = smoothstep(0.0, 0.2, normR) * smoothstep(1.0, 0.7, normR);
        
        vec3 finalColor = color * brightness + vec3(innerEdge * 0.5);
        float alpha = edgeFade * 0.9 + innerEdge * 0.35;
        
        finalColor = clamp(finalColor, 0.0, 3.0);
        
        gl_FragColor = vec4(finalColor, alpha);
      }
    `,
    transparent: true,
    side: THREE.DoubleSide,
    blending: THREE.AdditiveBlending,
    depthWrite: false
  })

  const disk = new THREE.Mesh(diskGeometry, diskMaterial)
  disk.rotation.x = Math.PI * 0.12  // More edge-on
  disk.rotation.z = Math.PI * 0.03
  scene.add(disk)

  // Thinner outer disk
  const disk2Geometry = new THREE.RingGeometry(3.2, 10, 192, 1)
  const disk2Mat = diskMaterial.clone()
  const disk2 = new THREE.Mesh(disk2Geometry, disk2Mat)
  disk2.rotation.x = Math.PI * 0.1
  disk2.rotation.z = Math.PI * 0.02
  scene.add(disk2)
}

const createGravitationalLensing = () => {
  if (!scene) return

  // Gravitational lensing arcs - light from behind black hole bending over the top
  // These should be prominent and curve OVER the black hole

  // Primary arc - most prominent, curving over the top
  const arc1Geom = new THREE.TorusGeometry(4.0, 0.6, 12, 72, Math.PI * 0.9)
  const arc1Mat = new THREE.MeshBasicMaterial({
    color: 0xffee99,
    transparent: true,
    opacity: 0.85,
    blending: THREE.AdditiveBlending
  })
  const arc1 = new THREE.Mesh(arc1Geom, arc1Mat)
  arc1.position.set(0.5, 1.2, 0)
  arc1.rotation.z = Math.PI * 0.2
  arc1.rotation.x = -0.3
  scene.add(arc1)

  // Brightest part - the "crown" at the very top
  const topGlowGeom = new THREE.TorusGeometry(3.0, 0.4, 10, 48, Math.PI * 0.7)
  const topGlowMat = new THREE.MeshBasicMaterial({
    color: 0xffffdd,
    transparent: true,
    opacity: 0.95,
    blending: THREE.AdditiveBlending
  })
  const topGlow = new THREE.Mesh(topGlowGeom, topGlowMat)
  topGlow.position.set(0.3, 0.5, 0.3)
  topGlow.rotation.z = Math.PI * 0.25
  topGlow.rotation.x = -0.2
  scene.add(topGlow)

  // Secondary lensed arc - wider
  const arc2Geom = new THREE.TorusGeometry(5.5, 0.4, 10, 56, Math.PI * 0.8)
  const arc2Mat = new THREE.MeshBasicMaterial({
    color: 0xffcc66,
    transparent: true,
    opacity: 0.6,
    blending: THREE.AdditiveBlending
  })
  const arc2 = new THREE.Mesh(arc2Geom, arc2Mat)
  arc2.position.set(0.8, 2.5, -0.3)
  arc2.rotation.z = Math.PI * 0.15
  arc2.rotation.x = -0.25
  scene.add(arc2)

  // Third arc - fainter
  const arc3Geom = new THREE.TorusGeometry(7.0, 0.25, 8, 40, Math.PI * 0.65)
  const arc3Mat = new THREE.MeshBasicMaterial({
    color: 0xff9933,
    transparent: true,
    opacity: 0.4,
    blending: THREE.AdditiveBlending
  })
  const arc3 = new THREE.Mesh(arc3Geom, arc3Mat)
  arc3.position.set(1.0, 4, -0.5)
  arc3.rotation.z = Math.PI * 0.1
  arc3.rotation.x = -0.3
  scene.add(arc3)

  // Fourth arc - very faint, highest
  const arc4Geom = new THREE.TorusGeometry(8.5, 0.15, 8, 32, Math.PI * 0.5)
  const arc4Mat = new THREE.MeshBasicMaterial({
    color: 0xff7711,
    transparent: true,
    opacity: 0.25,
    blending: THREE.AdditiveBlending
  })
  const arc4 = new THREE.Mesh(arc4Geom, arc4Mat)
  arc4.position.set(1.2, 5.5, -0.8)
  arc4.rotation.z = Math.PI * 0.06
  arc4.rotation.x = -0.35
  scene.add(arc4)
}

const createStarField = () => {
  if (!scene) return

  const starCount = 2000
  const positions = new Float32Array(starCount * 3)
  const colors = new Float32Array(starCount * 3)

  for (let i = 0; i < starCount; i++) {
    const i3 = i * 3
    const theta = Math.random() * Math.PI * 2
    const phi = Math.acos(2 * Math.random() - 1)
    const r = 50 + Math.random() * 100
    
    positions[i3] = r * Math.sin(phi) * Math.cos(theta)
    positions[i3 + 1] = r * Math.sin(phi) * Math.sin(theta)
    positions[i3 + 2] = r * Math.cos(phi)

    const t = Math.random()
    if (t > 0.9) {
      colors[i3] = 0.6; colors[i3 + 1] = 0.7; colors[i3 + 2] = 1.0
    } else if (t > 0.7) {
      colors[i3] = 1.0; colors[i3 + 1] = 0.9; colors[i3 + 2] = 0.7
    } else {
      colors[i3] = 1.0; colors[i3 + 1] = 1.0; colors[i3 + 2] = 1.0
    }
  }

  const geometry = new THREE.BufferGeometry()
  geometry.setAttribute('position', new THREE.BufferAttribute(positions, 3))
  geometry.setAttribute('color', new THREE.BufferAttribute(colors, 3))

  const material = new THREE.PointsMaterial({
    size: 0.3,
    vertexColors: true,
    transparent: true,
    opacity: 0.8
  })

  const stars = new THREE.Points(geometry, material)
  scene.add(stars)
}

const animate = () => {
  if (!renderer || !scene || !camera) return

  time.value += 0.016

  if (diskMaterial && diskMaterial.uniforms.time) {
    diskMaterial.uniforms.time.value = time.value
  }

  renderer.render(scene, camera)
  animationId = requestAnimationFrame(animate)
}

const handleResize = () => {
  if (!renderer || !camera || !containerRef.value) return

  const width = window.innerWidth
  const height = window.innerHeight * 0.68

  camera.aspect = width / height
  camera.updateProjectionMatrix()
  renderer.setSize(width, height)
}

onMounted(() => {
  initThree()
  animate()
  window.addEventListener('resize', handleResize)
})

onUnmounted(() => {
  window.removeEventListener('resize', handleResize)
  if (animationId) cancelAnimationFrame(animationId)
  if (renderer && containerRef.value) {
    containerRef.value.removeChild(renderer.domElement)
    renderer.dispose()
  }
})
</script>

<template>
  <div ref="containerRef" class="black-hole-container" />
</template>

<style scoped>
.black-hole-container {
  position: absolute;
  top: 0;
  left: 0;
  width: 100%;
  height: 100%;
  overflow: hidden;
}
</style>
