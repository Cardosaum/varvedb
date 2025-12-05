import * as THREE from 'three';

export function initHero3D() {
  const container = document.getElementById('hero-canvas');
  if (!container) return;

  // Cleanup existing canvas if any
  while (container.firstChild) {
    container.removeChild(container.firstChild);
  }

  // Scene setup
  const scene = new THREE.Scene();
  // Deep dark background to match site
  scene.fog = new THREE.FogExp2(0x0f1117, 0.03);

  const camera = new THREE.PerspectiveCamera(60, window.innerWidth / window.innerHeight, 0.1, 1000);
  // Position camera for a dramatic angle
  camera.position.set(0, 5, 12);
  camera.lookAt(0, 0, 0);

  const renderer = new THREE.WebGLRenderer({ alpha: true, antialias: true });
  renderer.setSize(window.innerWidth, window.innerHeight);
  renderer.setPixelRatio(Math.min(window.devicePixelRatio, 2)); // Cap pixel ratio for performance
  container.appendChild(renderer.domElement);

  // "Holographic Strata"
  // A stack of flowing lines representing the immutable log layers
  const group = new THREE.Group();
  
  // Geometry for a single "wave line"
  // We use a plane but will only render it as points or lines for a tech look
  const width = 40;
  const depth = 40;
  const widthSegments = 60;
  const depthSegments = 40;
  const geometry = new THREE.PlaneGeometry(width, depth, widthSegments, depthSegments);

  // Custom Shader Material for the "Data Flow" look
  // This allows us to animate vertices on the GPU for high performance and smooth look
  const vertexShader = `
    uniform float time;
    uniform float scrollY;
    varying float vElevation;
    varying vec2 vUv;

    void main() {
      vUv = uv;
      
      vec3 pos = position;
      
      // Create a moving wave effect
      // The "varve" layers undulate
      float wave1 = sin(pos.x * 0.2 + time * 0.5) * 1.0;
      float wave2 = cos(pos.y * 0.3 + time * 0.3) * 0.5;
      
      // Apply elevation
      pos.z += wave1 + wave2;
      
      // Save elevation for fragment shader coloring
      vElevation = pos.z;

      vec4 modelPosition = modelMatrix * vec4(pos, 1.0);
      vec4 viewPosition = viewMatrix * modelPosition;
      vec4 projectedPosition = projectionMatrix * viewPosition;

      gl_Position = projectedPosition;
      gl_PointSize = 2.0;
    }
  `;

  const fragmentShader = `
    uniform vec3 color;
    varying float vElevation;
    varying vec2 vUv;

    void main() {
      // Fade out at edges
      float alpha = 1.0 - smoothstep(0.3, 0.5, distance(vUv, vec2(0.5)));
      
      // Height-based glow
      float glow = smoothstep(-1.0, 2.0, vElevation);
      
      // Final color mixing
      vec3 finalColor = mix(color * 0.5, color * 2.0, glow);
      
      gl_FragColor = vec4(finalColor, alpha * 0.6);
    }
  `;

  const material = new THREE.ShaderMaterial({
    vertexShader,
    fragmentShader,
    uniforms: {
      time: { value: 0 },
      color: { value: new THREE.Color(0x38bdf8) }
    },
    transparent: true,
    depthWrite: false,
    blending: THREE.AdditiveBlending,
    wireframe: true // Try wireframe for the "grid" look
  });

  // Create two planes for a "sandwich" or "reflection" effect
  const planeTop = new THREE.Points(geometry, material);
  planeTop.rotation.x = -Math.PI / 2 + 0.2; // Tilted
  planeTop.position.y = -2;
  group.add(planeTop);

  // Add a second layer for depth
  const material2 = material.clone();
  material2.uniforms.color.value = new THREE.Color(0x4d5960); // Darker secondary color
  const planeBottom = new THREE.Points(geometry, material2);
  planeBottom.rotation.x = -Math.PI / 2 + 0.2;
  planeBottom.position.y = -5; // Lower
  group.add(planeBottom);

  scene.add(group);

  // Mouse interaction
  let mouseX = 0;
  let mouseY = 0;

  const windowHalfX = window.innerWidth / 2;
  const windowHalfY = window.innerHeight / 2;

  document.addEventListener('mousemove', (event) => {
    mouseX = (event.clientX - windowHalfX) * 0.0001;
    mouseY = (event.clientY - windowHalfY) * 0.0001;
  });

  // Animation Loop
  const clock = new THREE.Clock();

  function animate() {
    requestAnimationFrame(animate);
    const elapsedTime = clock.getElapsedTime();

    // Update shader uniforms
    material.uniforms.time.value = elapsedTime;
    material2.uniforms.time.value = elapsedTime + 2.0; // Offset

    // Subtle camera movement
    camera.position.x += (mouseX * 10 - camera.position.x) * 0.05;
    camera.position.y += (-mouseY * 10 + 5 - camera.position.y) * 0.05;
    camera.lookAt(0, 0, 0);

    // Rotate the group slightly
    group.rotation.y = Math.sin(elapsedTime * 0.1) * 0.1;

    renderer.render(scene, camera);
  }

  animate();

  // Handle Resize
  window.addEventListener('resize', () => {
    camera.aspect = window.innerWidth / window.innerHeight;
    camera.updateProjectionMatrix();
    renderer.setSize(window.innerWidth, window.innerHeight);
  });
}
