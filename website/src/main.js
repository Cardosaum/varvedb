import './style.css'

// Simple intersection observer for fade-in animations
const observerOptions = {
  root: null,
  rootMargin: '0px',
  threshold: 0.1
};

const observer = new IntersectionObserver((entries) => {
  entries.forEach(entry => {
    if (entry.isIntersecting) {
      entry.target.classList.add('visible');
      observer.unobserve(entry.target);
    }
  });
}, observerOptions);

document.addEventListener('DOMContentLoaded', () => {
  // Add fade-in class to sections
  const sections = document.querySelectorAll('section');
  sections.forEach(section => {
    section.classList.add('fade-in-section');
    observer.observe(section);
  });

  // Smooth scroll for anchor links
  document.querySelectorAll('a[href^="#"]').forEach(anchor => {
    anchor.addEventListener('click', function (e) {
      e.preventDefault();
      document.querySelector(this.getAttribute('href')).scrollIntoView({
        behavior: 'smooth'
      });
    });
  });

  // Copy to clipboard functionality
  const copyBtn = document.querySelector('.copy-btn');
  if (copyBtn) {
    copyBtn.addEventListener('click', async () => {
      const command = 'cargo add varvedb';
      try {
        await navigator.clipboard.writeText(command);
        
        // Visual feedback
        const originalIcon = copyBtn.innerHTML;
        copyBtn.innerHTML = `
          <svg xmlns="http://www.w3.org/2000/svg" fill="none" viewBox="0 0 24 24" stroke-width="2" stroke="currentColor" style="color: #4ade80;">
            <path stroke-linecap="round" stroke-linejoin="round" d="M4.5 12.75l6 6 9-13.5" />
          </svg>
        `;
        
        setTimeout(() => {
          copyBtn.innerHTML = originalIcon;
        }, 2000);
      } catch (err) {
        console.error('Failed to copy:', err);
      }
    });
  }

  // Interactive Architecture
  const interactives = document.querySelectorAll('[data-id]');
  
  interactives.forEach(el => {
    el.addEventListener('mouseenter', () => {
      const id = el.getAttribute('data-id');
      if (!id) return;

      // Highlight matching elements
      interactives.forEach(other => {
        if (other.getAttribute('data-id') === id) {
          other.classList.add('highlight');
        } else {
          other.classList.add('dim');
        }
      });
    });

    el.addEventListener('mouseleave', () => {
      // Reset all
      interactives.forEach(other => {
        other.classList.remove('highlight');
        other.classList.remove('dim');
      });
    });
  });
});
